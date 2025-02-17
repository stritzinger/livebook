defmodule Livebook.Runtime.ErlDist do
  # This module allows for initializing connected runtime nodes with
  # modules and processes necessary for evaluation.
  #
  # To ensure proper isolation between sessions, code evaluation may
  # take place in a separate Elixir runtime, which also makes it easy
  # to terminate the whole evaluation environment without stopping
  # Livebook. Both `Runtime.Standalone` and `Runtime.Attached`
  # do that and this module contains the shared functionality.
  #
  # To work with a separate node, we have to inject the necessary
  # Livebook modules there and also start the relevant processes
  # related to evaluation. Fortunately Erlang allows us to send
  # modules binary representation to the other node and load them
  # dynamically.
  #
  # For further details see `Livebook.Runtime.ErlDist.NodeManager`.

  # Defines the paths for necessary .beam files and application metadata used
  # for loading Elixir and Compiler modules into a remote node for runtime setup
  @elixir_ebin Application.app_dir(:elixir, "ebin")
  @elixir_app Application.app_dir(:elixir, "ebin/elixir.app")
  @compiler_ebin Application.app_dir(:compiler, "ebin")
  @compiler_app Application.app_dir(:compiler, "ebin/compiler.app")

  @doc """
  Livebook modules necessary for evaluation within a runtime node.
  """
  @spec livebook_required_modules() :: list(module())
  def livebook_required_modules() do
    [
      Livebook.Runtime.Definitions,
      Livebook.Runtime.Evaluator,
      Livebook.Runtime.Evaluator.IOProxy,
      Livebook.Runtime.Evaluator.Tracer,
      Livebook.Runtime.Evaluator.ObjectTracker,
      Livebook.Runtime.Evaluator.ClientTracker,
      Livebook.Runtime.Evaluator.Formatter,
      Livebook.Runtime.Evaluator.Doctests,
      Livebook.Intellisense,
      Livebook.Intellisense.Docs,
      Livebook.Intellisense.IdentifierMatcher,
      Livebook.Intellisense.SignatureMatcher,
      Livebook.Runtime.ErlDist,
      Livebook.Runtime.ErlDist.NodeManager,
      Livebook.Runtime.ErlDist.RuntimeServer,
      Livebook.Runtime.ErlDist.EvaluatorSupervisor,
      Livebook.Runtime.ErlDist.IOForwardGL,
      Livebook.Runtime.ErlDist.LoggerGLHandler,
      Livebook.Runtime.ErlDist.SmartCellGL,
      Livebook.Proxy.Adapter,
      Livebook.Proxy.Handler
    ]
  end

  @doc """
  Elixir modules required for running required Livebook modules within
  an Erlang runtime node.
  """
  @spec elixir_required_modules() :: list(module())
  def elixir_required_modules() do
    [Mix, Logger, Logger.Formatter]
  end

  @doc """
  Starts a runtime server on the specified node, ensuring the node is
  properly prepared for code evaluation.

  It checks if the necessary modules and processes are present on the target
  node and loads them if required.

  ## Options

    * `:node_manager_opts` - see `Livebook.Runtime.ErlDist.NodeManager.start/1`

    * `:runtime_server_opts` - see `Livebook.Runtime.ErlDist.RuntimeServer.start_link/1`

    * `:max_attempts` - The maximum number of attempts to initialize the runtime server.
    Defaults to `3`. Must be a positive integer.

    * `:retry_delay` - The delay in milliseconds between retry attempts when
    initialization fails. Defaults to `1000` milliseconds (1 second). Must be a
    non-negative integer.

  """
  @spec initialize(node(), keyword()) :: pid()
  def initialize(node, opts \\ []) do
    max_attempts = Keyword.get(opts, :max_attempts, 3)
    retry_delay = Keyword.get(opts, :retry_delay, 500)

    initialize_with_retries(node, opts, 0, max_attempts, retry_delay)
  end

  defp initialize_with_retries(node, opts, attempts, max_attempts, retry_delay) do
    case start_runtime_server(node, opts[:runtime_server_opts] || []) do
      {:ok, pid} ->
        pid

      {:error, :down} when attempts < max_attempts ->
        try do
          setup_runtime_node(node, opts)
        rescue
          error in RuntimeError ->
            raise RuntimeError, "Failed to initialize the runtime server: #{error.message}"

          other ->
            IO.puts(other)
        else
          _ ->
            :timer.sleep(retry_delay)
            initialize_with_retries(node, opts, attempts + 1, max_attempts, retry_delay)
        end

      {:error, :down} ->
        raise RuntimeError,
              "Exceeded maximum retry attempts (#{max_attempts}) to initialize the runtime server"
    end
  end

  defp setup_runtime_node(node, opts) do
    load_helper_module(node)

    unless module_loaded?(node, :elixir) do
      load_elixir_and_compiler(node)
      load_modules(node, elixir_required_modules())
      set_elixir_env(node)
    end

    unless module_loaded?(node, Livebook.Runtime.ErlDist.NodeManager) do
      load_modules(node, livebook_required_modules())
    end

    {:ok, _} = start_node_manager(node, opts[:node_manager_opts] || [])
    :ok
  end

  defp set_elixir_env(node) do
    :rpc.call(node, :application, :ensure_all_started, [:elixir])
    :rpc.call(node, :application, :set_env, [:logger, :truncate, 8192])
    :rpc.call(node, :application, :set_env, [:logger, :level, :info])
    :rpc.call(node, :application, :set_env, [:logger, :utc_log, true])
  end

  defp start_node_manager(node, opts) do
    :rpc.call(node, Livebook.Runtime.ErlDist.NodeManager, :start, [opts])
  end

  defp start_runtime_server(node, opts) do
    Livebook.Runtime.ErlDist.NodeManager.start_runtime_server(node, opts)
  end

  defp module_loaded?(node, module) do
    case :rpc.call(node, :code, :ensure_loaded, [module]) do
      {:module, _module} -> true
      _ -> false
    end
  end

  # Loads the necessary Elixir and Compiler modules into the specified runtime
  # node to ensure it can run Elixir-based processes.
  defp load_elixir_and_compiler(node) do
    @compiler_ebin
    |> extract_modules_from_path()
    |> then(&load_modules(node, &1))

    @elixir_ebin
    |> extract_modules_from_path()
    |> then(&load_modules(node, &1))

    :rpc.call(node, File, :mkdir, ["/tmp/ebin"])
    :rpc.call(node, Code, :append_path, ["/tmp/ebin"])

    load_app_file(node, @elixir_app, "/tmp/ebin/elixir.app")
    load_app_file(node, @compiler_app, "/tmp/ebin/compiler.app")
  end

  defp load_app_file(node, file, path) do
    case File.read(file) do
      {:ok, content} ->
        :rpc.call(node, File, :write, [path, content, []])
        :rpc.call(node, :application, :load, [:elixir])

      {:error, reason} ->
        raise "Failed to read #{file}: #{reason}"
    end
  end

  defp extract_modules_from_path(path) do
    for file <- File.ls!(path),
        Path.extname(file) == ".beam",
        do: file |> Path.rootname() |> String.to_atom()
  end

  defp load_modules(node, modules) do
    binary =
      modules
      |> Enum.map(&:code.get_object_code/1)
      |> :erlang.term_to_binary()
      |> :zlib.gzip()

    case :rpc.call(
           node,
           Livebook.Runtime.ErlDist.LoadCompressedModules,
           :load_compressed_modules,
           [binary]
         ) do
      :ok -> :ok
      _ -> raise "Failed to load #{inspect(modules)} on remote node"
    end
  end

  defp load_helper_module(node) do
    case :code.get_object_code(Livebook.Runtime.ErlDist.LoadCompressedModules) do
      {module, binary, filename} ->
        case :rpc.call(node, :code, :load_binary, [module, filename, binary]) do
          {:module, _} -> :ok
          _ -> raise "Failed to load #{inspect(module)} on remote node"
        end

      :error ->
        raise "Module #{inspect(Livebook.Runtime.ErlDist.LoadCompressedModules)} is not compiled or cannot be found"
    end
  end

  @spec unload_required_modules() :: list()
  @doc """
  Unloads the previously loaded Livebook modules from the caller node.
  """
  def unload_required_modules() do
    for module <- livebook_required_modules() do
      # If we attached, detached and attached again, there may still
      # be deleted module code, so purge it first.
      :code.purge(module)
      :code.delete(module)
    end
  end
end
