open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Common.SetArgs.BenchmarkSetArgs> |]

    benchmarks.Run argv |> ignore
    0
