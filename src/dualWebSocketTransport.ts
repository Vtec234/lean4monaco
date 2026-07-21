import { MessageTransports } from 'vscode-languageclient/lib/common/client.js'
import {
  DataCallback,
  Disposable,
  Emitter,
  MessageReader,
  NotificationMessage,
  PartialMessageInfo,
} from 'vscode-jsonrpc'
import {
  toSocket,
  WebSocketMessageReader,
  WebSocketMessageWriter,
} from 'vscode-ws-jsonrpc'

export type LspChannel = 'hi' | 'lo'

export type DualWebSocketTransport = {
  transports: MessageTransports
  close: () => void
}

const SET_SERVER_MESSAGE_PRIORITIZATION_METHOD =
  '$/lean4web/setServerMessagePrioritization'

const addChannelParameters = (
  baseUrl: string,
  session: string,
  channel: LspChannel,
) => {
  const url = new URL(baseUrl)
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error(`This is not a proper websocket url: ${baseUrl}`)
  }
  url.searchParams.set('session', session)
  url.searchParams.set('channel', channel)
  return url.toString()
}

const createSessionId = () => {
  const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16))
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
}

const waitForOpen = (socket: WebSocket) =>
  new Promise<void>((resolve, reject) => {
    const onOpen = () => {
      cleanUp()
      resolve()
    }
    const onError = () => {
      cleanUp()
      reject(new Error(`WebSocket connection to ${socket.url} failed`))
    }
    const onClose = (event: CloseEvent) => {
      cleanUp()
      reject(
        new Error(
          `WebSocket connection to ${socket.url} closed before opening (code ${event.code})`,
        ),
      )
    }
    const cleanUp = () => {
      socket.removeEventListener('open', onOpen)
      socket.removeEventListener('error', onError)
      socket.removeEventListener('close', onClose)
    }

    socket.addEventListener('open', onOpen)
    socket.addEventListener('error', onError)
    socket.addEventListener('close', onClose)
  })

class MergedMessageReader implements MessageReader {
  private readonly errorEmitter = new Emitter<Error>()
  private readonly closeEmitter = new Emitter<void>()
  private readonly partialMessageEmitter = new Emitter<PartialMessageInfo>()
  private readonly eventDisposables: Disposable[] = []
  private listenDisposables: Disposable[] = []
  private closed = false

  readonly onError = this.errorEmitter.event
  readonly onClose = this.closeEmitter.event
  readonly onPartialMessage = this.partialMessageEmitter.event

  constructor(
    private readonly readers: MessageReader[],
    private readonly closeSockets: () => void,
  ) {
    for (const reader of readers) {
      this.eventDisposables.push(
        reader.onError((error) => this.errorEmitter.fire(error)),
        reader.onPartialMessage((info) => this.partialMessageEmitter.fire(info)),
        reader.onClose(() => this.handleClose()),
      )
    }
  }

  listen(callback: DataCallback): Disposable {
    this.listenDisposables = this.readers.map((reader) => reader.listen(callback))
    return {
      dispose: () => {
        for (const disposable of this.listenDisposables) disposable.dispose()
        this.listenDisposables = []
      },
    }
  }

  dispose() {
    for (const disposable of this.listenDisposables) disposable.dispose()
    for (const disposable of this.eventDisposables) disposable.dispose()
    for (const reader of this.readers) reader.dispose()
    this.listenDisposables = []
    this.eventDisposables.length = 0
    this.errorEmitter.dispose()
    this.closeEmitter.dispose()
    this.partialMessageEmitter.dispose()
    this.closeSockets()
  }

  private handleClose() {
    if (this.closed) return
    this.closed = true
    this.closeSockets()
    this.closeEmitter.fire()
  }
}

/**
 * Open the two physical sockets used by one logical LSP connection.
 *
 * Messages from both sockets are merged by the reader. Client-originated
 * messages intentionally use only the low-priority channel for now.
 */
export const createDualWebSocketTransport = async (
  baseUrl: string,
  prioritizeServerMessages = true,
  onClose?: () => void,
): Promise<DualWebSocketTransport> => {
  const session = createSessionId()
  const urls: Record<LspChannel, string> = {
    hi: addChannelParameters(baseUrl, session, 'hi'),
    lo: addChannelParameters(baseUrl, session, 'lo'),
  }
  const hiSocket = new WebSocket(urls.hi)
  let loSocket: WebSocket
  try {
    loSocket = new WebSocket(urls.lo)
  } catch (error) {
    hiSocket.close()
    throw error
  }
  const sockets: Record<LspChannel, WebSocket> = { hi: hiSocket, lo: loSocket }
  let closed = false

  const close = () => {
    if (closed) return
    closed = true
    for (const socket of Object.values(sockets)) {
      if (
        socket.readyState === WebSocket.CONNECTING ||
        socket.readyState === WebSocket.OPEN
      ) {
        try {
          socket.close()
        } catch {
          // A connecting socket can finish closing before close() is invoked.
        }
      }
    }
    onClose?.()
  }

  for (const socket of Object.values(sockets)) {
    socket.addEventListener('error', close)
    socket.addEventListener('close', close)
  }

  try {
    await Promise.all([waitForOpen(sockets.hi), waitForOpen(sockets.lo)])
  } catch (error) {
    close()
    throw error
  }

  if (closed) {
    throw new Error('A dual-channel WebSocket closed during startup')
  }

  const hiRpcSocket = toSocket(sockets.hi)
  const loRpcSocket = toSocket(sockets.lo)
  const reader = new MergedMessageReader(
    [
      new WebSocketMessageReader(hiRpcSocket),
      new WebSocketMessageReader(loRpcSocket),
    ],
    close,
  )
  const writer = new WebSocketMessageWriter(loRpcSocket)
  const configurationMessage: NotificationMessage = {
    jsonrpc: '2.0',
    method: SET_SERVER_MESSAGE_PRIORITIZATION_METHOD,
    params: { enabled: prioritizeServerMessages },
  }
  try {
    await writer.write(configurationMessage)
  } catch (error) {
    close()
    throw error
  }

  return {
    transports: { reader, writer },
    close,
  }
}
