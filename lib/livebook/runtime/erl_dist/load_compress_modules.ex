defmodule Livebook.Runtime.ErlDist.LoadCompressedModules do
  @moduledoc """
  A helper module that provides a function to load compressed modules sent to a remote node.
  """

  @doc """
  Loads compressed module binaries sent as a gzipped binary.
  """
  @spec load_compressed_modules(binary()) :: :ok | {:error, String.t()}
  def load_compressed_modules(compressed_binary) do
    case :zlib.gunzip(compressed_binary) do
      {:error, _} ->
        {:error, "Failed to decompress binary"}

      decompressed ->
        modules = :erlang.binary_to_term(decompressed)
        load_modules(modules)
    end
  end

  defp load_modules([]), do: :ok

  defp load_modules([{module, binary, filename} | rest]) do
    case :code.load_binary(module, filename, binary) do
      {:module, _} -> load_modules(rest)
      _ -> {:error, "Failed to load #{inspect(module)}"}
    end
  end
end
