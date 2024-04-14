namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp

module ClCellExtensions =
    type ClCell<'a> with
        member this.ToHost(processor: DeviceCommandQueue<_>) =
            let res = Array.zeroCreate<'a> 1
            processor.Post(Msg.CreateToHostMsg(this, res))
            res.[0]

        member this.Free(processor: DeviceCommandQueue<_>) =
            processor.Post(Msg.CreateFreeMsg<_>(this))

        member this.ToHostAndFree(processor: DeviceCommandQueue<_>) =
            let result = this.ToHost processor
            this.Free processor

            result
