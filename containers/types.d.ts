/**
 * Type declarations for @cloudflare/containers SDK
 *
 * These types are based on the Cloudflare Containers documentation.
 * The actual SDK types will be provided when the package is installed.
 *
 * @see https://developers.cloudflare.com/containers/
 */

declare module '@cloudflare/containers' {
  /**
   * Container binding interface.
   * Represents a connection to a container running in Cloudflare's infrastructure.
   */
  export interface Container {
    /**
     * Send a fetch request to the container.
     * The request is routed to the container's default port.
     */
    fetch(request: Request): Promise<Response>

    /**
     * Get the container's unique identifier.
     */
    readonly id: string

    /**
     * Check if the container is running.
     */
    isRunning(): Promise<boolean>
  }

  /**
   * Container class for defining container behavior in wrangler.toml.
   * Extend this class to customize container settings.
   */
  export class Container {
    /**
     * Default port the container listens on.
     */
    static defaultPort?: number

    /**
     * Duration of inactivity before the container sleeps.
     * Format: '10m', '1h', etc.
     */
    static sleepAfter?: string
  }

  /**
   * Get a container instance for a specific session.
   *
   * @param container - The container binding from env
   * @param sessionId - Session identifier for container affinity
   * @returns Container instance
   */
  export function getContainer(container: Container, sessionId?: string): Container
}
