
import { Worker } from "bullmq";
import { env } from "../config/env.js";
import { FILE_PROCESSING_QUEUE } from "../infrastructure/queue/file-processing.queue.js";
import { getObject } from "../infrastructure/storage/storage.service.js";
import { updateJobProcessingMetadata } from "../modules/jobs/job.repository.js";
import {
  changeJobStatus,
  failJob,
  getJobById,
  recordJobAttempt,
} from "../modules/jobs/job.service.js";

interface FileProcessingJobData {
  jobId: string;
  fileId: string;
}

export const fileProcessingWorker = new Worker<FileProcessingJobData>(
  FILE_PROCESSING_QUEUE,
  async (job) => {
    const { jobId, fileId } = job.data;
    const attemptNumber = job.attemptsMade + 1;

    console.log(
      `[worker] Starting attempt ${attemptNumber} for job ${jobId}`,
    );

    try {
      const currentJob = await getJobById(jobId);

      if (
        currentJob.status === "FAILED" ||
        currentJob.status === "COMPLETED"
      ) {
        console.log(
          `[worker] Skipping terminal job ${jobId}: ${currentJob.status}`,
        );
        return;
      }

      if (
        currentJob.status !== "PENDING" &&
        currentJob.status !== "PROCESSING"
      ) {
        throw new Error(
          `Job ${jobId} cannot be processed from status ${currentJob.status}`,
        );
      }

      await recordJobAttempt(jobId);

      const startedAt = Date.now();

      if (currentJob.status === "PENDING") {
        await changeJobStatus(jobId, "PROCESSING");
      }

      const processingJob = await getJobById(jobId);
      const file = processingJob.file;

      if (!file || file.id !== fileId) {
        throw new Error("File associated with job was not found");
      }

      const buffer = await getObject(file.storageKey);

      if (buffer.length === 0) {
        throw new Error("Stored file is empty");
      }

      const durationMs = Date.now() - startedAt;

      await updateJobProcessingMetadata(
        jobId,
        BigInt(buffer.length),
        durationMs,
      );

      console.log(`[worker] Processing file: ${file.originalName}`);
      console.log(`[worker] File size: ${buffer.length} bytes`);
      console.log(`[worker] MIME type: ${file.mimeType}`);
      console.log(`[worker] Processing duration: ${durationMs} ms`);

      await changeJobStatus(jobId, "COMPLETED");
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Unknown processing error";

      console.error(
        `[worker] Processing failed for job ${jobId}: ${message}`,
      );

      const isFinalAttempt =
        job.attemptsMade + 1 >= (job.opts.attempts ?? 1);

      if (isFinalAttempt) {
        try {
          await failJob(jobId, message);
        } catch (failureError) {
          console.error(
            `[worker] Failed to mark job ${jobId} as FAILED:`,
            failureError,
          );
        }
      }

      throw error;
    }
  },
  {
    connection: {
      url: env.REDIS_URL,
    },
    concurrency: env.WORKER_CONCURRENCY,
    limiter: {
      max: 10,
      duration: 1000,
    },
  },
);

fileProcessingWorker.on("completed", (job) => {
  console.log(`[worker] Job completed: ${job.id}`);
});

fileProcessingWorker.on("failed", (job, error) => {
  console.error(
    `[worker] Job failed: ${job?.id ?? "unknown"}`,
    error,
  );
});

fileProcessingWorker.on("error", (error) => {
  console.error("[worker] Worker error:", error);
});

fileProcessingWorker.on("stalled", (jobId) => {
  console.warn(`[worker] Job stalled: ${jobId}`);
});

fileProcessingWorker.on("active", (job) => {
  console.log(`[worker] Job active: ${job.id}`);
});
