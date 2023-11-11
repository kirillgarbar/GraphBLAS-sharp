namespace GraphBLAS.FSharp.Backend.Operations

open Brahma.FSharp
open Microsoft.FSharp.Quotations
open GraphBLAS.FSharp.Backend.Common
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Objects.ClContextExtensions

module internal SpMV =
    let runTo
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =

        let localMemorySize =
            clContext.ClDevice.LocalMemSize / 1<Byte>

        let localPointersArraySize = workGroupSize + 1

        let localMemoryLeft =
            localMemorySize
            - localPointersArraySize * sizeof<int>

        let localValuesArraySize =
            Utils.getClArrayOfOptionTypeSize localMemoryLeft

        let multiplyValues =
            <@ fun (ndRange: Range1D) matrixLength (matrixColumns: ClArray<int>) (matrixValues: ClArray<'a>) (vectorValues: ClArray<'b option>) (intermediateArray: ClArray<'c option>) ->

                let i = ndRange.GlobalID0
                let value = matrixValues.[i]
                let column = matrixColumns.[i]

                if i < matrixLength then
                    intermediateArray.[i] <- (%mul) (Some value) vectorValues.[column] @>

        let reduceValuesByRows =
            <@ fun (ndRange: Range1D) (numberOfRows: int) (intermediateArray: ClArray<'c option>) (matrixPtr: ClArray<int>) (outputVector: ClArray<'c option>) ->

                let gid = ndRange.GlobalID0
                let lid = ndRange.LocalID0

                let localPtr = localArray<int> localPointersArraySize

                let localValues =
                    localArray<'c option> localValuesArraySize

                if gid <= numberOfRows then
                    let threadsPerBlock =
                        min (numberOfRows - gid + lid) workGroupSize //If number of rows left is lesser than number of threads in a block

                    localPtr.[lid] <- matrixPtr.[gid]

                    if lid = 0 then
                        localPtr.[threadsPerBlock] <- matrixPtr.[gid + threadsPerBlock]

                    barrierLocal ()

                    let workEnd = localPtr.[threadsPerBlock]
                    let mutable blockLowerBound = localPtr.[0]
                    let numberOfBlocksFitting = localValuesArraySize / threadsPerBlock
                    let workPerIteration = threadsPerBlock * numberOfBlocksFitting

                    let mutable sum: 'c option = None

                    while blockLowerBound < workEnd do
                        let mutable index = blockLowerBound + lid

                        barrierLocal ()
                        //Loading values to the local memory
                        for block in 0 .. numberOfBlocksFitting - 1 do
                            if index < workEnd then
                                localValues.[lid + block * threadsPerBlock] <- intermediateArray.[index]
                                index <- index + threadsPerBlock

                        barrierLocal ()
                        //Reduction
                        //Check if any part of the row is loaded into local memory on this iteration
                        if (localPtr.[lid + 1] > blockLowerBound
                            && localPtr.[lid] < blockLowerBound + workPerIteration) then
                            let rowStart = max (localPtr.[lid] - blockLowerBound) 0

                            let rowEnd =
                                min (localPtr.[lid + 1] - blockLowerBound) workPerIteration

                            for j in rowStart .. rowEnd - 1 do
                                let newSum = (%add) sum localValues.[j] //For some reason sum <- (%add) ... causes Brahma exception
                                sum <- newSum

                        blockLowerBound <- blockLowerBound + workPerIteration

                    if gid < numberOfRows then
                        outputVector.[gid] <- sum @>

        let multiplyValues = clContext.Compile multiplyValues
        let reduceValuesByRows = clContext.Compile reduceValuesByRows

        fun (queue: MailboxProcessor<_>) (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) (result: ClArray<'c option>) ->

            let matrixLength = matrix.Values.Length

            let ndRange1 =
                Range1D.CreateValid(matrixLength, workGroupSize)

            let ndRange2 =
                Range1D.CreateValid(matrix.RowCount, workGroupSize)

            let intermediateArray =
                clContext.CreateClArrayWithSpecificAllocationMode<'c option>(DeviceOnly, matrixLength)

            let multiplyValues = multiplyValues.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        multiplyValues.KernelFunc
                            ndRange1
                            matrixLength
                            matrix.Columns
                            matrix.Values
                            vector
                            intermediateArray)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(multiplyValues))

            let reduceValuesByRows = reduceValuesByRows.GetKernel()

            queue.Post(
                Msg.MsgSetArguments
                    (fun () ->
                        reduceValuesByRows.KernelFunc
                            ndRange2
                            matrix.RowCount
                            intermediateArray
                            matrix.RowPointers
                            result)
            )

            queue.Post(Msg.CreateRunMsg<_, _>(reduceValuesByRows))

            queue.Post(Msg.CreateFreeMsg intermediateArray)

            queue.PostAndReply(Msg.MsgNotifyMe)

    let run
        (add: Expr<'c option -> 'c option -> 'c option>)
        (mul: Expr<'a option -> 'b option -> 'c option>)
        (clContext: ClContext)
        workGroupSize
        =
        let runTo = runTo add mul clContext workGroupSize

        fun (queue: MailboxProcessor<_>) allocationMode (matrix: ClMatrix.CSR<'a>) (vector: ClArray<'b option>) ->

            let result =
                clContext.CreateClArrayWithSpecificAllocationMode<'c option>(allocationMode, matrix.RowCount)

            runTo queue matrix vector result

            result
