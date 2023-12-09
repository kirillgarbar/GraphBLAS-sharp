open GraphBLAS.FSharp.Benchmarks
open BenchmarkDotNet.Running

[<EntryPoint>]
let main argv =
    let benchmarks =
        BenchmarkSwitcher [| typeof<Common.Scan.ScanBenchmarkWithoutTransferInt32>
                             typeof<Common.Scan.ScanOldBenchmarkWithoutTransferInt32> |]

    benchmarks.Run argv |> ignore
    0
