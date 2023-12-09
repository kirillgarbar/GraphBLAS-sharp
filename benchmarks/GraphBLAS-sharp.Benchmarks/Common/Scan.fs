module GraphBLAS.FSharp.Benchmarks.Common.Scan

open FsCheck
open BenchmarkDotNet.Attributes
open Brahma.FSharp
open GraphBLAS.FSharp.Backend.Quotes
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Benchmarks
open GraphBLAS.FSharp.Tests
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ArraysExtensions
open GraphBLAS.FSharp.Objects.ClContextExtensions
open GraphBLAS.FSharp.Objects.ClCellExtensions

[<AbstractClass>]
[<IterationCount(100)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.MinMaxMean>)>]
type Benchmarks(
    buildFunToBenchmark) =

    let mutable funToBenchmark = None

    member val DeviceArray = Unchecked.defaultof<ClArray<int>> with get, set

    member val HostArray = Unchecked.defaultof<int []> with get, set

    member val ResultCell = Unchecked.defaultof<ClCell<int>> with get, set

    member val ArraySize = 10000

    [<ParamsSource("AvailableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf $"%A{e}")
        p

    static member AvailableContexts = Utils.availableContexts

    member this.FunToBenchmark =
        match funToBenchmark with
        | None ->
            let x = buildFunToBenchmark this.OclContext this.WorkGroupSize
            funToBenchmark <- Some x
            x
        | Some x -> x

    member this.Scan() =
        this.FunToBenchmark this.Processor this.DeviceArray

    member this.ClearInputArray() =
        this.ResultCell.Free this.Processor
        this.DeviceArray.FreeAndWait this.Processor

    member this.CreateArray()  =
        this.HostArray <- Array.create this.ArraySize 1

    member this.LoadArrayToGPU() =
        this.DeviceArray <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)

    abstract member GlobalSetup: unit -> unit

    abstract member IterationSetup: unit -> unit

    abstract member Benchmark: unit -> unit

    abstract member IterationCleanup: unit -> unit

    abstract member GlobalCleanup: unit -> unit

type ScanBenchmarkWithoutTransferInt32() =

        inherit Benchmarks(
            GraphBLAS.FSharp.Backend.Common.Scan.standardExcludeInPlace)

        [<GlobalSetup>]
        override this.GlobalSetup() =
            this.CreateArray()

        [<IterationSetup>]
        override this.IterationSetup() =
            this.LoadArrayToGPU()
            this.Processor.PostAndReply Msg.MsgNotifyMe

        [<Benchmark>]
        override this.Benchmark() =
            this.ResultCell <- this.Scan()
            this.Processor.PostAndReply Msg.MsgNotifyMe

        [<IterationCleanup>]
        override this.IterationCleanup() =
            this.ClearInputArray()

        [<GlobalCleanup>]
        override this.GlobalCleanup() = ()

type ScanOldBenchmarkWithoutTransferInt32() =

        inherit Benchmarks(
            GraphBLAS.FSharp.Backend.Common.PrefixSum.standardExcludeInPlace)

        [<GlobalSetup>]
        override this.GlobalSetup() =
            this.CreateArray()

        [<IterationSetup>]
        override this.IterationSetup() =
            this.LoadArrayToGPU()
            this.Processor.PostAndReply Msg.MsgNotifyMe

        [<Benchmark>]
        override this.Benchmark() =
            this.ResultCell <- this.Scan()
            this.Processor.PostAndReply Msg.MsgNotifyMe

        [<IterationCleanup>]
        override this.IterationCleanup() =
            this.ClearInputArray()

        [<GlobalCleanup>]
        override this.GlobalCleanup() = ()
