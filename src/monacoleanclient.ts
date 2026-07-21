import { LanguageClientWrapper, WorkerConfigDirect, WebSocketConfigOptions, WebSocketConfigOptionsUrl, WorkerConfigOptions } from 'monaco-editor-wrapper'
import { MonacoLanguageClient } from 'monaco-languageclient'
import { LanguageClientOptions } from 'vscode-languageclient/node'
import { Message } from 'vscode-jsonrpc'
import { displayNotification } from './vscode-lean4/vscode-lean4/src/utils/notifs'
import merge from 'lodash/merge'
import { createDualWebSocketTransport } from './dualWebSocketTransport'

export const setupMonacoClient = (
  options: WebSocketConfigOptions | WebSocketConfigOptionsUrl | WorkerConfigOptions | WorkerConfigDirect,
  moreClientOptions?: LanguageClientOptions
) => {
  return async (clientOptions: LanguageClientOptions) => {
    const mergedClientOptions = merge({}, clientOptions, moreClientOptions)
    mergedClientOptions.connectionOptions = {
      ...mergedClientOptions.connectionOptions,
      messageStrategy: {
        handleMessage: (message: any, next: (message: Message) => void) => {
          if (message.error) {
            // TODO: Handle Lean errors correctly
            displayNotification("Error", message.error.message)
            next(message) // remove this to prevent propagating the message
          } else {
            next(message)
          }
        }
      }
    }

    if (options.$type === 'WebSocketUrl') {
      let client: MonacoLanguageClient | undefined
      let transportClosed = false
      const connectionProvider = {
        get: async () => {
          const transport = await createDualWebSocketTransport(options.url, () => {
            if (transportClosed) return
            transportClosed = true
            options.stopOptions?.onCall(client)
          })
          return transport.transports
        }
      }

      client = new MonacoLanguageClient({
        name: 'Lean 4',
        clientOptions: mergedClientOptions,
        connectionProvider,
      })
      await client.start()
      options.startOptions?.onCall(client)
      ;(client as any)._serverProcess = { stderr: { on: () => {} }}
      return client
    }

    const languageClientWrapper = new LanguageClientWrapper()
    await languageClientWrapper.init({
      languageClientConfig: {
        languageId: 'lean4',
        options,
        clientOptions: mergedClientOptions
      }
    })
    await languageClientWrapper?.start()
    const client = languageClientWrapper.getLanguageClient()!;
    (client as any)._serverProcess = { stderr: { on: () => {} }}
    return client
  }
}
