module GraphBLAS.FSharp.Benchmarks.Common.SetArgs

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

[<IterationCount(20)>]
[<WarmupCount(10)>]
[<Config(typeof<Configs.MinMaxMean>)>]
type BenchmarkSetArgs() =

    //let kernel = <@ fun (ndRange: Range1D) (inputArray1: ClArray<int>) (inputArray2: ClArray<int>) (inputArray3: ClArray<int>) (inputArray4: ClArray<int>) (inputArray5: ClArray<int>) (inputArray6: ClArray<int>) (inputArray7: ClArray<int>) (inputArray8: ClArray<int>) (inputArray9: ClArray<int>) (inputArray10: ClArray<int>) -> () @>
    let kernel = <@ fun (ndRange: Range1D) (inputArray1: ClArray<int>) -> () @>

    member val kernelCompiled = None with get, set

    member val DeviceArray1 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray2 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray3 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray4 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray5 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray6 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray7 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray8 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray9 = Unchecked.defaultof<ClArray<int>> with get, set
    member val DeviceArray10 = Unchecked.defaultof<ClArray<int>> with get, set

    member val HostArray = Unchecked.defaultof<int []> with get, set

    member val ArraySize = 1000000

    member val Iterations = 1000

    [<ParamsSource("AvailableContexts")>]
    member val OclContextInfo = Unchecked.defaultof<Utils.BenchmarkContext * int> with get, set

    member this.OclContext: ClContext = (fst this.OclContextInfo).ClContext
    member this.WorkGroupSize = snd this.OclContextInfo

    member this.Processor =
        let p = (fst this.OclContextInfo).Queue
        p.Error.Add(fun e -> failwithf $"%A{e}")
        p

    static member AvailableContexts = Utils.availableContexts

    member this.ClearInputArrays() =
        this.DeviceArray1.FreeAndWait this.Processor
        this.DeviceArray2.FreeAndWait this.Processor
        this.DeviceArray3.FreeAndWait this.Processor
        this.DeviceArray4.FreeAndWait this.Processor
        this.DeviceArray5.FreeAndWait this.Processor
        this.DeviceArray6.FreeAndWait this.Processor
        this.DeviceArray7.FreeAndWait this.Processor
        this.DeviceArray8.FreeAndWait this.Processor
        this.DeviceArray9.FreeAndWait this.Processor
        this.DeviceArray10.FreeAndWait this.Processor

    member this.CreateArray()  =
        this.HostArray <- Array.create this.ArraySize 1

    member this.LoadArraysToGPU() =
        this.DeviceArray1 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray2 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray3 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray4 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray5 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray6 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray7 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray8 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray9 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)
        this.DeviceArray10 <- this.OclContext.CreateClArrayWithSpecificAllocationMode(DeviceOnly, this.HostArray)

    [<GlobalSetup>]
    member this.GlobalSetup() =
        this.kernelCompiled <- Some (this.OclContext.Compile kernel)
        this.CreateArray()
        this.LoadArraysToGPU()

    [<IterationSetup>]
    member this.IterationSetup() =
        this.Processor.PostAndReply Msg.MsgNotifyMe

    [<Benchmark>]
    member this.Benchmark() =
        let mutable i = 0
        while i < this.Iterations do
            let ndRange =
                Range1D.CreateValid(1, this.WorkGroupSize)

            let kernel = this.kernelCompiled.Value.GetKernel()

            //this.Processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange this.DeviceArray1 this.DeviceArray2 this.DeviceArray3 this.DeviceArray4 this.DeviceArray5 this.DeviceArray6 this.DeviceArray7 this.DeviceArray8 this.DeviceArray9 this.DeviceArray10))
            this.Processor.Post(Msg.MsgSetArguments(fun () -> kernel.KernelFunc ndRange this.DeviceArray1))

            this.Processor.Post(Msg.CreateRunMsg<_, _>(kernel))
        
            this.Processor.PostAndReply Msg.MsgNotifyMe
            i <- i + 1

    [<IterationCleanup>]
    member this.IterationCleanup() = ()
        

    [<GlobalCleanup>]
    member this.GlobalCleanup() = 
        this.ClearInputArrays()
