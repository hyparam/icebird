/**
 * @typedef {Response | Record<string, unknown>} RouteValue
 * @typedef {RouteValue | (() => RouteValue)} Route
 */

/**
 * Builds a fetch mock that returns canned responses keyed by URL.
 * Each route is a static value/Response or a thunk returning one.
 * Inspect `calls` to assert what was sent.
 *
 * @param {Record<string, Route>} routes
 * @returns {{ fn: (url: string, init?: RequestInit) => Promise<Response>, calls: Array<{url: string, init: RequestInit | undefined}> }}
 */
export function makeFetch(routes) {
  /** @type {Array<{url: string, init: RequestInit | undefined}>} */
  const calls = []
  /**
   * @param {string} url
   * @param {RequestInit} [init]
   * @returns {Promise<Response>}
   */
  function fn(url, init) {
    calls.push({ url, init })
    const route = routes[url]
    if (route === undefined) {
      return Promise.resolve(new Response(JSON.stringify({
        error: { code: 404, type: 'NotFound', message: `no route for ${url}` },
      }), { status: 404 }))
    }
    const value = typeof route === 'function' ? route() : route
    if (value instanceof Response) return Promise.resolve(value)
    return Promise.resolve(new Response(JSON.stringify(value), {
      status: 200,
      headers: { 'content-type': 'application/json' },
    }))
  }
  return { fn, calls }
}
