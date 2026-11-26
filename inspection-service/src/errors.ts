/** 统一的业务 / HTTP 错误。 */
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export function badRequest(code: string, message: string, details?: Record<string, unknown>): never {
  throw new ApiError(400, code, message, details);
}
export function notFound(code: string, message: string, details?: Record<string, unknown>): never {
  throw new ApiError(404, code, message, details);
}
export function conflict(code: string, message: string, details?: Record<string, unknown>): never {
  throw new ApiError(409, code, message, details);
}
