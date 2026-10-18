import { prisma } from "../../infrastructure/database/prisma.js";
import type { Prisma } from "../../generated/prisma/client.js";

export async function createJob(data: Prisma.JobCreateInput) {
  return prisma.job.create({
    data,
  });
}

export async function findJobById(id: string) {
  return prisma.job.findUnique({
    where: {
      id,
    },
    include: {
      file: true,
    },
  });
}

export async function findJobsByFileId(fileId: string) {
  return prisma.job.findMany({
    where: {
      fileId,
    },
    orderBy: {
      createdAt: "desc",
    },
  });
}

export async function findActiveJobByFileId(fileId: string) {
  return prisma.job.findFirst({
    where: {
      fileId,
      status: {
        in: ["PENDING", "PROCESSING"],
      },
    },
    orderBy: {
      createdAt: "desc",
    },
  });
}

export async function incrementJobAttempts(id: string) {
  return prisma.job.update({
    where: {
      id,
    },
    data: {
      attempts: {
        increment: 1,
      },
    },
  });
}

export async function resetJobAttempts(id: string) {
  return prisma.job.update({
    where: {
      id,
    },
    data: {
      attempts: 0,
      error: null,
    },
  });
}

export async function updateJobStatus(
  id: string,
  status: NonNullable<Prisma.JobUpdateInput["status"]>,
) {
  return prisma.job.update({
    where: {
      id,
    },
    data: {
      status,
    },
  });
}

export async function updateJobStatusIfCurrent(
  id: string,
  currentStatus: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED",
  nextStatus: "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED",
) {
  return prisma.job.updateMany({
    where: {
      id,
      status: currentStatus,
    },
    data: {
      status: nextStatus,
    },
  });
}

export async function markJobFailed(id: string, error: string) {
  return prisma.job.update({
    where: {
      id,
    },
    data: {
      status: "FAILED",
      error,
    },
  });
}

export async function updateJobProcessingMetadata(
  id: string,
  processedBytes: bigint,
  processingTimeMs: number,
) {
  return prisma.job.update({
    where: {
      id,
    },
    data: {
      processedBytes,
      processingTimeMs,
    },
  });
}


export async function updateJobAndFileStatus(
  jobId: string,
  fileId: string,
  status: NonNullable<Prisma.JobUpdateInput["status"]>,
  error?: string,
  expectedStatuses: (
    "PENDING" | "PROCESSING" | "COMPLETED" | "FAILED"
  )[] = ["PENDING", "PROCESSING", "FAILED"],
) {
  return prisma.$transaction(async (tx) => {
    const result = await tx.job.updateMany({
      where: {
        id: jobId,
        fileId,
        status: {
          in: expectedStatuses,
        },
      },
      data: {
        status,
        ...(error !== undefined ? { error } : {}),
      },
    });

    if (result.count !== 1) {
      throw new Error(
        `Job ${jobId} was not updated because its status or file association changed`,
      );
    }

    await tx.file.update({
      where: { id: fileId },
      data: { status },
    });

    return tx.job.findUniqueOrThrow({
      where: { id: jobId },
      include: { file: true },
    });
  });
}

