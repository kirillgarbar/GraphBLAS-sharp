namespace GraphBLAS.FSharp.Objects

open Brahma.FSharp

module ClCellExtensions =
    type ClCell<'a> with
        member this.ToHost(processor: RawCommandQueue) =
            let res = Array.zeroCreate<'a> 1
            processor.ToHost(this, res, true)
            res.[0]

        member this.Free() = this.Dispose()

        member this.ToHostAndFree(processor: RawCommandQueue) =
            let result = this.ToHost processor
            this.Dispose()

            result
