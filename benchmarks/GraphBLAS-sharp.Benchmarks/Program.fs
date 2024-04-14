open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Algorithms.BFS.BFSPushPullWithoutTransferBenchmarkBool> |]

    benchmarks.Run argv |> ignore
    0
