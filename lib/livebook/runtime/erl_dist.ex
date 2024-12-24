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
    opts |> IO.inspect()
    # First, we attempt to communicate with the node manager, in case
    # there is one running. Otherwise, the node is not initialized,
    # so we need to initialize it and try again
    case start_runtime_server(node, opts[:runtime_server_opts] || []) do
      {:ok, pid} ->
        pid

      {:error, :down} ->
        unless modules_loaded?(node) do
          load_required_modules(node)
        end

        {:ok, _} = start_node_manager(node, opts[:node_manager_opts] || [])
        {:ok, pid} = start_runtime_server(node, opts[:runtime_server_opts] || [])
        pid
    end
  end

  defp load_required_modules(node) do
    for module <- required_modules() do
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
    node |> IO.inspect()
    :rpc.call(node, Livebook.Runtime.ErlDist.NodeManager, :start, [opts])
  end

  defp start_runtime_server(node, opts) do
    Livebook.Runtime.ErlDist.NodeManager.start_runtime_server(node, opts)
  end

  defp modules_loaded?(node) do
    :rpc.call(node, Code, :ensure_loaded?, [Livebook.Runtime.ErlDist.NodeManager])
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
