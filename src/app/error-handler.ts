
import type { FastifyError, FastifyInstance } from "fastify";
import { AppError } from "../shared/errors/app-error.js";

export function registerErrorHandler(app: FastifyInstance): void {
  app.setErrorHandler((error: FastifyError, request, reply) => {
    if (error instanceof AppError) {
      request.log.warn(
        {
          code: error.code,
          statusCode: error.statusCode,
        },
        error.message,
      );

      return reply.status(error.statusCode).send({
        error: {
          code: error.code,
          message: error.message,
        },
      });
    }

    if (error.code === "FST_ERR_VALIDATION") {
      request.log.warn(
        {
          code: error.code,
          validation: error.validation,
        },
        "Request validation failed",
      );

      return reply.status(400).send({
        error: {
          code: "VALIDATION_ERROR",
          message: "Request validation failed",
        },
      });
    }

    const statusCode =
      typeof error.statusCode === "number" &&
        error.statusCode >= 400 &&
        error.statusCode < 600
        ? error.statusCode
        : 500;

    request.log.error(
      {
        err: error,
        statusCode,
      },
      "Request failed",
    );

    return reply.status(statusCode).send({
      error: {
        code:
          statusCode === 415
            ? "UNSUPPORTED_MEDIA_TYPE"
            : statusCode === 500
              ? "INTERNAL_SERVER_ERROR"
              : "REQUEST_ERROR",
        message:
          statusCode === 500
            ? "An unexpected error occurred"
            : error.message,
      },
    });
  });
}
