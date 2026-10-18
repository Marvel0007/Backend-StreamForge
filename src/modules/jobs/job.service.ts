import { AppError } from "../../shared/errors/app-error.js";
import {
  createJob,
  findActiveJobByFileId,
  findJobById,
  findJobsByFileId,
  incrementJobAttempts,
  resetJobAttempts,
  updateJobAndFileStatus,
} from "./job.repository.js";
import { findFileById } from "../files/file.repository.js";
import { fileProcessingQueue } from "../../infrastructure/queue/file-processing.queue.js";

type JobStatus = "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED";

const VALID_STATUS_TRANSITIONS: Record<JobStatus, JobStatus[]> = {
  PENDING: ["PROCESSING", "FAILED"],
  PROCESSING: ["COMPLETED", "FAILED"],
  COMPLETED: [],
  FAILED: ["PENDING"],
};

export async function createFileProcessingJob(fileId: string) {
  console.log("[job] createFileProcessingJob called:", fileId);

  const file = await findFileById(fileId);

  if (!file) {
    throw new AppError("File not found", 404, "FILE_NOT_FOUND");
  }

  const activeJob = await findActiveJobByFileId(fileId);

  console.log("[job] active job found:", activeJob?.id ?? "NONE");

  if (activeJob) {
    throw new AppError(
      "File already has an active processing job",
      409,
      "ACTIVE_JOB_EXISTS",
    );
  }

  const job = await createJob({
    type: "FILE_PROCESSING",
    file: {
      connect: {
        id: fileId,
      },
    },
  });

  console.log("[job] new job created:", job.id);

  await fileProcessingQueue.add(
    "file-processing",
    {
      jobId: job.id,
      fileId: file.id,
    },
    {
      attempts: job.maxAttempts,
      backoff: {
        type: "exponential",
        delay: 1000,
      },
      removeOnComplete: false,
      removeOnFail: false,
    },
  );

  return job;
}

export async function getJobById(id: string) {
  const job = await findJobById(id);

  if (!job) {
    throw new AppError("Job not found", 404, "JOB_NOT_FOUND");
  }

  return job;
}

export async function getJobsByFileId(fileId: string) {
  const file = await findFileById(fileId);

  if (!file) {
    throw new AppError("File not found", 404, "FILE_NOT_FOUND");
  }

  return findJobsByFileId(fileId);
}

export async function changeJobStatus(id: string, status: JobStatus) {
  const job = await getJobById(id);

  const currentStatus = job.status as JobStatus;

  if (!VALID_STATUS_TRANSITIONS[currentStatus].includes(status)) {
    throw new AppError(
      `Invalid job status transition: ${currentStatus} -> ${status}`,
      409,
      "INVALID_JOB_STATUS_TRANSITION",
    );
  }

  return updateJobAndFileStatus(
    id,
    job.fileId,
    status,
    undefined,
    [currentStatus],
  );
}

export async function recordJobAttempt(id: string) {
  return incrementJobAttempts(id);
}

export async function failJob(id: string, error: string) {
  const job = await getJobById(id);

  if (job.status === "FAILED") {
    return job;
  }

  return updateJobAndFileStatus(id, job.fileId, "FAILED", error);
}


export async function retryJob(jobId: string) {
  const job = await getJobById(jobId);

  if (job.status !== "FAILED") {
    throw new AppError(
      "Only failed jobs can be retried",
      409,
      "INVALID_RETRY_STATUS",
    );
  }

  const existingQueueJob = await fileProcessingQueue.getJob(jobId);

  if (existingQueueJob) {
    const queueState = await existingQueueJob.getState();

    if (queueState === "active") {
      throw new AppError(
        "The queue job is still active and cannot be retried yet",
        409,
        "QUEUE_JOB_ACTIVE",
      );
    }

    await existingQueueJob.remove();
  }

  await resetJobAttempts(jobId);

  await changeJobStatus(jobId, "PENDING");

  try {
    await fileProcessingQueue.add(
      "file-processing",
      {
        jobId: job.id,
        fileId: job.file.id,
      },
      {
        jobId: job.id,
        attempts: job.maxAttempts,
        backoff: {
          type: "exponential",
          delay: 1000,
        },
        removeOnComplete: false,
        removeOnFail: false,
      },
    );
  } catch (error) {
    try {
      await changeJobStatus(jobId, "FAILED");
    } catch (rollbackError) {
      console.error(
        `[job] Failed to roll back retry for ${jobId}:`,
        rollbackError,
      );
    }

    throw error;
  }

  return getJobById(jobId);
}

export async function cancelJob(jobId: string) {
  const job = await getJobById(jobId);

  if (job.status !== "PENDING") {
    throw new AppError(
      "Only pending jobs can be cancelled",
      409,
      "INVALID_CANCEL_STATUS",
    );
  }

  const existingQueueJob = await fileProcessingQueue.getJob(jobId);

  if (!existingQueueJob) {
    throw new AppError(
      "Queue job not found; cancellation could not be confirmed",
      409,
      "QUEUE_JOB_NOT_FOUND",
    );
  }

  const queueState = await existingQueueJob.getState();

  if (queueState !== "waiting" && queueState !== "delayed") {
    throw new AppError(
      "Job has already started or is not cancellable",
      409,
      "JOB_NOT_CANCELLABLE",
    );
  }

  try {
    await existingQueueJob.remove();
  } catch {
    throw new AppError(
      "Job started before it could be cancelled; try again after checking its status",
      409,
      "JOB_CANCELLATION_RACE",
    );
  }

  return changeJobStatus(jobId, "FAILED");
}

