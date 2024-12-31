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

  @elixir_ebin "path/to/elixir/1.18/lib/elixir/ebin"
  @elixir_app_path "path/to/elixir/1.18/lib/elixir/ebin/elixir.app"
  @compiler_path "path/to/erlang/26.2.2/lib/compiler-8.4.1/ebin"
  @compiler_app_path "path/to/erlang/26.2.2/lib/compiler-8.4.1/ebin/compiler.app"

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
    node |> IO.inspect()
    # First, we attempt to communicate with the node manager, in case
    # there is one running. Otherwise, the node is not initialized,
    # so we need to initialize it and try again
    case start_runtime_server(node, opts[:runtime_server_opts] || []) do
      {:ok, pid} ->
        pid

      {:error, :down} ->
        case modules_loaded?(node) do
          {:error, _} -> load_required_modules(node,  required_modules())
          _ -> :ok
        end
        case elixir_modules_loaded?(node) do
          {:error, _} ->
            load_required_modules(node,  elixir_required_modules())
            load_elixir_and_compiler(node)
          _ -> :ok
        end

        {:ok, _} = start_node_manager(node, opts[:node_manager_opts] || [])
        {:ok, pid} = start_runtime_server(node, opts[:runtime_server_opts] || [])
        pid
      other ->
        other |> IO.inspect()
    end
  end

  defp load_required_modules(node, modules) do
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


  defp load_elixir_and_compiler(node) do
    for file <- File.ls!(@elixir_ebin) do
      if Path.extname(file) == ".beam" do
        {module, binary, filename} = :code.get_object_code(String.to_atom(Path.rootname(file)))
        :rpc.call(node, :code, :load_binary, [module, filename, binary])
      end
    end

    for file <- File.ls!(@compiler_path) do
      if Path.extname(file) == ".beam" do
        {module, binary, filename} = :code.get_object_code(String.to_atom(Path.rootname(file)))
        :rpc.call(node, :code, :load_binary, [module, filename, binary])
      end
    end

    :rpc.call(node, File, :mkdir, ["/tmp/ebin"])
    :rpc.call(node, Code, :append_path, ["/tmp/ebin"])

    File.read!(@elixir_app_path)
    |> :rpc.call(node, File, :write, ["/tmp/ebin/elixir.app", []])

    File.read!(@compiler_app_path)
    |> :rpc.call(node, File, :write, ["/tmp/ebin/compiler.app", []])

    :rpc.call(node, :application, :load, [:elixir])
    :rpc.call(node, :application, :load, [:compiler])

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

  defp modules_loaded?(node) do
    :rpc.call(node, :code, :ensure_loaded, [Livebook.Runtime.ErlDist.NodeManager])
  end

  defp elixir_modules_loaded?(node) do
    :rpc.call(node, :code, :ensure_loaded, [:elixir])
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
