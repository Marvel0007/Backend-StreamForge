import { fileProcessingQueue } from "./job.queue.js";

export async function enqueueFileProcessing(
  jobId: string,
  fileId: string,
): Promise<void> {
  await fileProcessingQueue.add(
    "file-processing",
    {
      jobId,
      fileId,
    },
    {
      jobId,
      attempts: 3,
      backoff: {
        type: "exponential",
        delay: 1000,
      },
      removeOnComplete: 100,
      removeOnFail: 500,
    },
  );
}
