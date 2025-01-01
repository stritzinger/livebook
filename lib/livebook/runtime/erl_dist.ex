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

  @doc """
  Livebook modules necessary for evaluation within a runtime node.
  """

  @elixir_ebin Application.app_dir(:elixir, "ebin")
  @elixir_app Application.app_dir(:elixir, "ebin/elixir.app")
  @compiler_ebin Application.app_dir(:compiler, "ebin")
  @compiler_app Application.app_dir(:compiler, "ebin/compiler.app")

  @spec required_modules() :: list(module())
  def required_modules() do
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

  @spec elixir_required_modules() :: list(module())
  def elixir_required_modules() do
    [
      Mix,
      GenServer,
      Keyword,
      DynamicSupervisor,
      Enum,
      Access,
      Process,
      Supervisor.Default,
      Map,
      Logger.Formatter,
      Logger,
      IO,
      Inspect,
      Inspect.Opts,
      Inspect.Algebra,
      Inspect.PID,
      Kernel,
      Inspect.Atom,
      Macro,
      Inspect.BitString,
      String,
      Code.Identifier,
      Regex,
      IO.ANSI,
      Application,
      String.Tokenizer,
      ArgumentError,
      Code,
      System,
      File,
      List,
      File.Stat,
      Base,
      Path,
      List.Chars,
      List.Chars.BitString
    ]
  end

  @doc """
  Starts a runtime server on the given node.

  If necessary, the required modules are loaded into the given node
  and the node manager process is started with `node_manager_opts`.

  ## Options

    * `:node_manager_opts` - see `Livebook.Runtime.ErlDist.NodeManager.start/1`

    * `:runtime_server_opts` - see `Livebook.Runtime.ErlDist.RuntimeServer.start_link/1`

  """
  @spec initialize(node(), keyword()) :: pid()
  def initialize(node, opts \\ []) do
    IO.inspect("initialize, node:")
    node |> IO.inspect()
    # First, we attempt to communicate with the node manager, in case
    # there is one running. Otherwise, the node is not initialized,
    # so we need to initialize it and try again
    case start_runtime_server(node, opts[:runtime_server_opts] || []) do
      {:ok, pid} ->
        pid

      {:error, :down} ->
        case modules_loaded?(node, [:elixir]) do
          {:error, _} ->
            load_elixir_and_compiler(node)
            load_required_modules(node, elixir_required_modules())
            set_elixir_env(node)

          _ ->
            :ok
        end

        case modules_loaded?(node, [Livebook.Runtime.ErlDist.NodeManager]) do
          {:error, _} -> load_required_modules(node, required_modules())
          _ -> :ok
        end

        {:ok, _} = start_node_manager(node, opts[:node_manager_opts] || [])
        {:ok, pid} = start_runtime_server(node, opts[:runtime_server_opts] || [])
        pid

      other ->
        IO.inspect("initialize, node:")
        other |> IO.inspect()
    end
  end

  defp load_required_modules(node, modules) do
    IO.inspect("load_required_modules")

    for module <- modules do
      {_module, binary, filename} = :code.get_object_code(module)

      case :rpc.call(node, :code, :load_binary, [module, filename, binary]) do
        {:module, _} ->
          :ok

        {:error, reason} ->
          local_otp = :erlang.system_info(:otp_release)
          remote_otp = :rpc.call(node, :erlang, :system_info, [:otp_release])

          if local_otp != remote_otp do
            raise RuntimeError,
                  "failed to load #{inspect(module)} module into the remote node," <>
                    " potentially due to Erlang/OTP version mismatch, reason: #{inspect(reason)} (local #{local_otp} != remote #{remote_otp})"
          else
            raise RuntimeError,
                  "failed to load #{inspect(module)} module into the remote node, reason: #{inspect(reason)}"
          end
      end
    end
  end

  defp start_node_manager(node, opts) do
    :rpc.call(node, Livebook.Runtime.ErlDist.NodeManager, :start, [opts])
  end

  defp start_runtime_server(node, opts) do
    Livebook.Runtime.ErlDist.NodeManager.start_runtime_server(node, opts)
  end

  defp modules_loaded?(node, modules) do
    :rpc.call(node, :code, :ensure_loaded, modules)
  end

  defp load_elixir_and_compiler(node) do
    load_ebin_files(node, @compiler_ebin)
    load_ebin_files(node, @elixir_ebin)

    :rpc.call(node, File, :mkdir, ["/tmp/ebin"])
    :rpc.call(node, Code, :append_path, ["/tmp/ebin"])

    load_app_file(node, @elixir_app, "/tmp/ebin/elixir.app")
    load_app_file(node, @compiler_app, "/tmp/ebin/compiler.app")
  end

  defp load_ebin_files(node, path) do
    for file <- File.ls!(path) do
      case file |> Path.extname() do
        ".beam" ->
          {module, binary, filename} =
            :code.get_object_code(String.to_atom(file |> Path.rootname()))

          :rpc.call(node, :code, :load_binary, [module, filename, binary])

        _ ->
          :ok
      end
    end
  end

  defp load_app_file(node, file, path) do
    case File.read(file) do
      {:ok, content} ->
        :rpc.call(node, File, :write, [path, content, []])
        :rpc.call(node, :application, :load, [:elixir])

      {:error, reason} ->
        IO.puts("Failed to read: #{reason}")
    end
  end

  defp set_elixir_env(node) do
    :rpc.call(node, :application, :ensure_all_started, [:elixir])
    :rpc.call(node, :application, :set_env, [:logger, :truncate, 8192])
    :rpc.call(node, :application, :set_env, [:logger, :level, :info])
    :rpc.call(node, :application, :set_env, [:logger, :utc_log, true])
  end

  @doc """
  Unloads the previously loaded Livebook modules from the caller node.
  """
  def unload_required_modules() do
    for module <- required_modules() do
      # If we attached, detached and attached again, there may still
      # be deleted module code, so purge it first.
      :code.purge(module)
      :code.delete(module)
    end
  end
end
