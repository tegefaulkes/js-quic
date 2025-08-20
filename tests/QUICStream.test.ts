import type { ClientCryptoOps, ServerCryptoOps } from '#types.js';
import type { TLSConfigs } from './utils.js';
import type QUICConnection from '#QUICConnection.js';
import Logger, { formatting, LogLevel, StreamHandler } from '@matrixai/logger';
import { test } from '@fast-check/jest';
import { firstValueFrom } from 'rxjs';
import * as testsUtils from './utils.js';
import { generateTLSConfig } from './utils.js';
import QUICServer from '#QUICServer.js';
import QUICClient from '#QUICClient.js';
import QUICStream from '#QUICStream.js';
import * as utils from '#utils.js';

async function consumeReadable(stream: QUICStream) {
  for await (const _chunk of stream.readable) {
    // Do nothing, only consume to completion
  }
}

describe('QUICStream', () => {
  const _logger = new Logger(`QUICStream Test`, LogLevel.SILENT, [
    new StreamHandler(
      formatting.format`${formatting.level}:${formatting.keys}:${formatting.msg}`,
    ),
  ]);
  const loggerClient = new Logger(`QUICClient`, LogLevel.WARN, [
    new StreamHandler(
      formatting.format`${formatting.level}:${formatting.keys}:${formatting.msg}`,
    ),
  ]);
  const loggerServer = new Logger(`QUICServer`, LogLevel.WARN, [
    new StreamHandler(
      formatting.format`${formatting.level}:${formatting.keys}:${formatting.msg}`,
    ),
  ]);
  const defaultType = 'RSA';
  const localhost = '127.0.0.1';
  // This has to be set up asynchronously due to key generation
  const serverCrypto: ServerCryptoOps = {
    sign: testsUtils.signHMAC,
    verify: testsUtils.verifyHMAC,
  };
  let key: ArrayBuffer;
  const clientCrypto: ClientCryptoOps = {
    randomBytes: testsUtils.randomBytes,
  };
  let socketCleanMethods: ReturnType<typeof testsUtils.socketCleanupFactory>;

  let tlsConfig: TLSConfigs;
  let server: QUICServer;
  let serverConnection: QUICConnection;
  let client: QUICClient;
  let clientConnection: QUICConnection;

  // We need to test the stream-making
  beforeEach(async () => {
    key = await testsUtils.generateKeyHMAC();
    socketCleanMethods = testsUtils.socketCleanupFactory();

    tlsConfig = await generateTLSConfig(defaultType);
    server = new QUICServer({
      crypto: {
        key,
        ops: serverCrypto,
      },
      logger: loggerServer.getChild(QUICServer.name),
      config: {
        key: tlsConfig.leafKeyPairPEM.privateKey,
        cert: tlsConfig.leafCertPEM,
        verifyPeer: false,
      },
    });
    socketCleanMethods.extractSocket(server);
    const connectionP = firstValueFrom(server.connection$, {
      defaultValue: undefined,
    });
    await server.start({
      host: localhost,
    });
    client = await QUICClient.createQUICClient({
      host: localhost,
      port: server.port,
      localHost: localhost,
      crypto: {
        ops: clientCrypto,
      },
      logger: loggerClient.getChild(QUICClient.name),
      config: {
        verifyPeer: false,
      },
    });
    socketCleanMethods.extractSocket(client);
    const connection = await connectionP;
    if (connection == null) throw Error('Connection is missing');
    serverConnection = connection;
    clientConnection = client.connection;
  });
  afterEach(async () => {
    await server.stop({ force: true });
    await client.destroy({ force: true });
    await socketCleanMethods.stopSockets();
  });

  // Easy paths
  test('should create a stream on client', async () => {
    // Note that no stream is created on the server side unless a message is sent.
    // Can create a stream
    const clientStream = clientConnection.newStream();
    expect(clientStream).toBeInstanceOf(QUICStream);
    // StreamId should be the initial client initiated bidi
    expect(clientStream.id).toBe(0b00);
  });
  test('should create a stream on server', async () => {
    // Note that no stream is created on the server side unless a message is sent.
    // Can create a stream
    const serverStream = serverConnection.newStream();
    expect(serverStream).toBeInstanceOf(QUICStream);
    // StreamId should be the initial server initiated bidi
    expect(serverStream.id).toBe(0b01);
  });
  test('should trigger complement stream on first message sent on client', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    expect(serverStream).toBeInstanceOf(QUICStream);
    expect(serverStream.id).toBe(0b00);
  });
  test('should trigger complement stream on first message sent on server', async () => {
    const clientStreamP = firstValueFrom(clientConnection.stream$);
    const serverStream = serverConnection.newStream();
    const serverWriter = serverStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the client side.
    await serverWriter.write(Buffer.from('message'));
    const clientStream = await clientStreamP;
    expect(clientStream).toBeInstanceOf(QUICStream);
    expect(clientStream.id).toBe(0b01);
  });
  test('should send data over stream', async () => {
    const messages = [
      'The ',
      'Quick ',
      'Brown ',
      'Fox ',
      'Jumped ',
      'Over ',
      'The ',
      'Lazy ',
      'Dog.',
    ];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    for (const message of messages) {
      await clientWriter.write(Buffer.from(message));
    }
    await clientWriter.close();
    const serverStream = await serverStreamP;
    let receivedData = '';
    for await (const chunk of serverStream.readable) {
      receivedData += chunk.toString();
    }
    expect(receivedData).toBe(messages.join(''));
  });
  test('should send large amount of data over stream', async () => {
    // Large amounts of data will block the writes until the buffers are drained.
    // So we need a non-blocking way of sending and receiving the data at the same time.
    // Default stream capacity is 13,500 bytes.
    const message = Buffer.alloc(
      20000,
      'The Quick Brown Fox Jumped Over The Lazy Dog.',
    );
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const handleServerStreamP = (async () => {
      const serverStream = await serverStreamP;
      let receivedData = '';
      for await (const chunk of serverStream.readable) {
        receivedData += chunk.toString();
      }
      return receivedData;
    })();
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    await clientWriter.write(message);
    await clientWriter.close();
    const receivedData = await handleServerStreamP;
    expect(receivedData).toBe(message.toString());
  });
  test('should handle multiple streams on client', async () => {
    const message = Buffer.alloc(
      20000,
      'The Quick Brown Fox Jumped Over The Lazy Dog.',
    );
    const streamsNum = 10;
    const serverStreamPromises: Array<Promise<string>> = [];
    const serverStreamsCreatedProm = utils.promise<void>();
    let serverStreamCount = 0;
    serverConnection.stream$.subscribe((stream) => {
      serverStreamPromises.push(
        (async () => {
          let receivedData = '';
          for await (const chunk of stream.readable) {
            receivedData += chunk.toString();
          }
          return receivedData;
        })(),
      );
      serverStreamCount++;
      if (serverStreamCount >= streamsNum) serverStreamsCreatedProm.resolveP();
    });
    // Start creating streams concurrently
    const waitProm = utils.promise();
    const clientStreamPromises: Array<Promise<void>> = [];
    for (let i = 0; i < streamsNum; i++) {
      clientStreamPromises.push(
        (async () => {
          await waitProm.p;
          const stream = clientConnection.newStream();
          const writer = stream.writable.getWriter();
          await writer.write(message);
          await writer.close();
        })(),
      );
    }

    // Now we let everything run concurrently
    waitProm.resolveP();
    await Promise.all(clientStreamPromises);
    await serverStreamsCreatedProm.p;
    const results = await Promise.all(serverStreamPromises);
    for (const result of results) {
      expect(result).toBe(message.toString());
    }
  });
  test('should handle multiple streams on server', async () => {
    const message = Buffer.alloc(
      20000,
      'The Quick Brown Fox Jumped Over The Lazy Dog.',
    );
    const streamsNum = 10;
    const clientStreamPromises: Array<Promise<string>> = [];
    const clientStreamsCreatedProm = utils.promise<void>();
    let clientStreamCount = 0;
    clientConnection.stream$.subscribe((stream) => {
      clientStreamPromises.push(
        (async () => {
          let receivedData = '';
          for await (const chunk of stream.readable) {
            receivedData += chunk.toString();
          }
          return receivedData;
        })(),
      );
      clientStreamCount++;
      if (clientStreamCount >= streamsNum) clientStreamsCreatedProm.resolveP();
    });
    // Start creating streams concurrently
    const waitProm = utils.promise();
    const serverStreamPromises: Array<Promise<void>> = [];
    for (let i = 0; i < streamsNum; i++) {
      serverStreamPromises.push(
        (async () => {
          await waitProm.p;
          const stream = serverConnection.newStream();
          const writer = stream.writable.getWriter();
          await writer.write(message);
          await writer.close();
        })(),
      );
    }

    // Now we let everything run concurrently
    waitProm.resolveP();
    await Promise.all(serverStreamPromises);
    await clientStreamsCreatedProm.p;
    const results = await Promise.all(clientStreamPromises);
    for (const result of results) {
      expect(result).toBe(message.toString());
    }
  });

  // Problem paths
  test('should handle writeable stream abort with data sent', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    await clientWriter.abort(new Error('some error'));
    const serverStream = await serverStreamP;
    const readP = (async () => {
      for await (const _chunk of serverStream.readable) {
        // Consume stream data
      }
    })();
    await expect(readP).rejects.toThrow();
  });
  test('should handle writeable stream abort with no data sent', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.abort(new Error('some error'));
    const serverStream = await serverStreamP;
    const readP = (async () => {
      for await (const _chunk of serverStream.readable) {
        // Consume data
      }
    })();
    await firstValueFrom(serverStream.readableComplete$);
    // FIXME: check actual error
    await expect(readP).rejects.toThrow();
  });
  test('should handle writeable stream abort while data blocked', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.alloc(20000, 'message'));
    await clientWriter.abort(new Error('some error'));

    const waitProm = utils.promise();
    const serverStream = await serverStreamP;
    const readP = (async () => {
      for await (const _chunk of serverStream.readable) {
        await waitProm.p;
      }
    })();
    // Waiting for the readable stream to end despite not being consumed
    await firstValueFrom(serverStream.readableComplete$);
    waitProm.resolveP();
    // FIXME: check actual error
    await expect(readP).rejects.toThrow();
  });
  test('should handle readable stream cancel with data sent', async () => {
    const message = Buffer.from('message');
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(message);
    const serverStream = await serverStreamP;
    const serverReader = serverStream.readable.getReader();
    await serverReader.read();
    await serverReader.cancel(new Error('some error'));
    await firstValueFrom(clientStream.writableComplete$);
    // TODO: use actual error
    await expect(clientWriter.write(message)).rejects.toThrow();
  });
  // It's not possible to cancel the readable stream before data is sent
  //  because we need data to be sent to create the stream in the first place.

  test('stream should complete after kill', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    serverStream.kill();

    // TODO: proper error
    await expect(consumeReadable(serverStream)).rejects.toThrow();
    await expect(consumeReadable(clientStream)).rejects.toThrow();

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('stream should complete after kill with FWC', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    await clientWriter.close();
    const serverStream = await serverStreamP;
    await consumeReadable(serverStream);
    serverStream.kill();

    // TODO: proper error
    await expect(consumeReadable(clientStream)).rejects.toThrow();

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('stream should complete after kill with RWC', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    await serverStream.writable.getWriter().close();
    await consumeReadable(clientStream);
    serverStream.kill();

    // TODO: proper error
    await expect(consumeReadable(serverStream)).rejects.toThrow();

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('stream should complete after kill with RRC', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    await serverStream.readable.cancel(new Error('some error'));
    await consumeReadable(serverStream);
    serverStream.kill();

    // TODO: proper error
    await expect(consumeReadable(clientStream)).rejects.toThrow();

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('stream should complete after kill', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    serverStream.kill();

    // TODO: proper error
    await expect(consumeReadable(serverStream)).rejects.toThrow();
    await expect(consumeReadable(clientStream)).rejects.toThrow();

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);

    // Further kills does nothing
    serverStream.kill();
    serverStream.kill();
    serverStream.kill();
  });

  test('should gracefully drain a connection of streams', async () => {
    const streams: Array<QUICStream> = [];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    streams.push(clientStream);
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    clientWriter.releaseLock();
    streams.push(await serverStreamP);
    const serverForwardStream = serverConnection.newStream();
    streams.push(serverForwardStream);
    const clientReverseStream = firstValueFrom(clientConnection.stream$);
    const serverForwardWriter = serverForwardStream.writable.getWriter();
    await serverForwardWriter.write(Buffer.from('message'));
    serverForwardWriter.releaseLock();
    streams.push(await clientReverseStream);
    const streamsCloseP = Promise.all(
      streams.map(async (stream) => {
        await stream.writable.close();
        for await (const _chunk of stream.readable) {
          // Just consume the stream data
        }
        await firstValueFrom(stream.complete$);
      }),
    );
    const endingStreamsP = clientConnection.endStreams(false);
    await streamsCloseP;
    await endingStreamsP;

    // TODO: check if new streams are rejected
    expect(() => clientConnection.newStream()).toThrow();
    const newServerStream = serverConnection.newStream();
    const newServerStreamWriter = newServerStream.writable.getWriter();
    await newServerStreamWriter.write(Buffer.from('message'));

    // The new stream should be rejected automatically after the first message
    await firstValueFrom(newServerStream.writableComplete$);
    await firstValueFrom(newServerStream.readableComplete$);
    await firstValueFrom(newServerStream.complete$);
  });

  test('calling clientConnection.endStreams should clean up streams', async () => {
    const streams: Array<QUICStream> = [];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    streams.push(clientStream);
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    streams.push(await serverStreamP);
    const serverForwardStream = serverConnection.newStream();
    streams.push(serverForwardStream);
    const clientReverseStream = firstValueFrom(clientConnection.stream$);
    const serverForwardWriter = serverForwardStream.writable.getWriter();
    await serverForwardWriter.write(Buffer.from('message'));
    streams.push(await clientReverseStream);
    const streamsCloseP = Promise.all(
      streams.map((v) => firstValueFrom(v.complete$)),
    );

    await clientConnection.endStreams(true);
    await streamsCloseP;
    expect(clientConnection.openStreams).toBe(0);
  });
  test('calling serverConnection.endStreams should clean up streams', async () => {
    const streams: Array<QUICStream> = [];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    streams.push(clientStream);
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    streams.push(await serverStreamP);
    const serverForwardStream = serverConnection.newStream();
    streams.push(serverForwardStream);
    const clientReverseStream = firstValueFrom(clientConnection.stream$);
    const serverForwardWriter = serverForwardStream.writable.getWriter();
    await serverForwardWriter.write(Buffer.from('message'));
    streams.push(await clientReverseStream);
    const streamsCloseP = Promise.all(
      streams.map((v) => firstValueFrom(v.complete$)),
    );

    await serverConnection.endStreams(true);
    await streamsCloseP;
    expect(clientConnection.openStreams).toBe(0);
  });
  test('calling concurrent endStreams should clean up streams', async () => {
    const streams: Array<QUICStream> = [];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    streams.push(clientStream);
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    streams.push(await serverStreamP);
    const serverForwardStream = serverConnection.newStream();
    streams.push(serverForwardStream);
    const clientReverseStream = firstValueFrom(clientConnection.stream$);
    const serverForwardWriter = serverForwardStream.writable.getWriter();
    await serverForwardWriter.write(Buffer.from('message'));
    streams.push(await clientReverseStream);
    const streamsCloseP = Promise.all(
      streams.map((v) => firstValueFrom(v.complete$)),
    );

    await Promise.all([
      serverConnection.endStreams(true),
      clientConnection.endStreams(true),
    ]);
    await streamsCloseP;
    expect(clientConnection.openStreams).toBe(0);
  });
  test('shutting down client should clean up streams', async () => {
    const streams: Array<QUICStream> = [];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    streams.push(clientStream);
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    streams.push(await serverStreamP);
    const serverForwardStream = serverConnection.newStream();
    streams.push(serverForwardStream);
    const clientReverseStream = firstValueFrom(clientConnection.stream$);
    const serverForwardWriter = serverForwardStream.writable.getWriter();
    await serverForwardWriter.write(Buffer.from('message'));
    streams.push(await clientReverseStream);
    const streamsCloseP = Promise.all(
      streams.map((v) => firstValueFrom(v.complete$)),
    );

    await client.destroy({ isApp: true, errorCode: 1, force: true });
    await streamsCloseP;
    expect(clientConnection.openStreams).toBe(0);
  });
  test('shutting down server should clean up streams', async () => {
    const streams: Array<QUICStream> = [];
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    streams.push(clientStream);
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    streams.push(await serverStreamP);
    const serverForwardStream = serverConnection.newStream();
    streams.push(serverForwardStream);
    const clientReverseStream = firstValueFrom(clientConnection.stream$);
    const serverForwardWriter = serverForwardStream.writable.getWriter();
    await serverForwardWriter.write(Buffer.from('message'));
    streams.push(await clientReverseStream);
    const streamsCloseP = Promise.all(
      streams.map((v) => firstValueFrom(v.complete$)),
    );

    await server.stop({ isApp: true, errorCode: 1, force: true });
    await streamsCloseP;
    expect(clientConnection.openStreams).toBe(0);
  });

  describe('stream completion tests', () => {
    // Here we have a bunch of ways the forward and reverse streams can end. They can end gracefully
    //  with a writable close. Alternatively, the streams can be errored with a writable abort or a
    //  readable cancel. We need to test each combination of this. The 3 cases are
    //  - writable close (WC)
    //  - writable abort (WA)
    //  - readable cancel (RC)

    // Testing a single stream completion
    test('should complete with FWC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.close();
      const serverStream = await serverStreamP;
      const readP = (async () => {
        for await (const _chunk of serverStream.readable) {
          // Consume stream data
        }
      })();
      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await readP;
    });
    test('should complete with FWA', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.abort(new Error('some error'));
      const serverStream = await serverStreamP;
      const readP = (async () => {
        for await (const _chunk of serverStream.readable) {
          // Consume stream data
        }
      })();
      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      // FIXME: check actual error
      await expect(readP).rejects.toThrow();
    });
    test('should complete with FRA', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      const serverReader = serverStream.readable.getReader();
      const readResult = await serverReader.read();
      expect(readResult.done).toBeFalse();
      await serverReader.cancel(new Error('some error'));
      serverReader.releaseLock();

      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(clientStream.writableComplete$);
      // FIXME: check actual error
      await expect(
        clientWriter.write(Buffer.from('message')),
      ).rejects.toThrow();
      // Canceled readable will just complete
      await consumeReadable(serverStream);
    });

    // Testing concurrent stream completion from both ends
    test('should complete with FWC-FRC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      const serverReader = serverStream.readable.getReader();
      const readResult = await serverReader.read();
      expect(readResult.done).toBeFalse();
      await Promise.all([
        serverReader.cancel(new Error('some error')),
        clientWriter.close(),
      ]);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(clientStream.writableComplete$);
    });
    test('should complete with FWA-FRC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      const serverReader = serverStream.readable.getReader();
      const readResult = await serverReader.read();
      expect(readResult.done).toBeFalse();
      await Promise.all([
        serverReader.cancel(new Error('some error')),
        clientWriter.abort(new Error('some error')),
      ]);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(clientStream.writableComplete$);
    });

    // Both forward and reverse streams completing should trigger QUICStream completion
    test('should fully complete with FWC, RWC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.close();
      const serverStream = await serverStreamP;
      const serverWriter = serverStream.writable.getWriter();
      await serverWriter.write(Buffer.from('message'));
      await serverWriter.close();

      await consumeReadable(clientStream);
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });

    test('should fully complete with FWA, RWC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.abort(new Error('some error'));
      const serverStream = await serverStreamP;
      const serverWriter = serverStream.writable.getWriter();
      await serverWriter.write(Buffer.from('message'));
      await serverWriter.close();

      await consumeReadable(clientStream);
      // TODO: proper error
      await expect(consumeReadable(serverStream)).rejects.toThrow();

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    test('should fully complete with FWC, RWA', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.close();
      const serverStream = await serverStreamP;
      const serverWriter = serverStream.writable.getWriter();
      await serverWriter.write(Buffer.from('message'));
      await serverWriter.abort(new Error('some error'));

      // TODO: proper error
      await expect(consumeReadable(clientStream)).rejects.toThrow();
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    test('should fully complete with FRC, RWC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      const serverWriter = serverStream.writable.getWriter();
      await serverWriter.write(Buffer.from('message'));
      await serverWriter.close();
      await serverStream.readable.cancel(new Error('some error'));

      await consumeReadable(clientStream);
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);

      // TODO: proper errors
      await expect(
        clientWriter.write(Buffer.from('message')),
      ).rejects.toThrow();
    });
    test('should fully complete with FWA, RWA', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.abort(new Error('some error'));
      const serverStream = await serverStreamP;
      const serverWriter = serverStream.writable.getWriter();
      await serverWriter.write(Buffer.from('message'));
      await serverWriter.abort(new Error('some error'));

      // TODO: proper error
      await expect(consumeReadable(clientStream)).rejects.toThrow();
      // TODO: proper error
      await expect(consumeReadable(serverStream)).rejects.toThrow();

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    // FIXME: The client readable stream should error since it was cancelled - check webstream behaviour here
    test('should fully complete with FWC, RRC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      await clientWriter.close();
      const serverStream = await serverStreamP;
      await clientStream.readable.cancel(new Error('some error'));

      // Canceled readable will just complete
      await consumeReadable(clientStream);
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    test('should fully complete with FRC, RWA', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      await serverStream.readable.cancel(new Error('some error'));
      await serverStream.writable.abort(new Error('some error'));

      // TODO: proper error
      await expect(consumeReadable(clientStream)).rejects.toThrow();
      // Canceled readable will just complete
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    test('should fully complete with FWA, RRC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      await clientWriter.abort(new Error('some error'));
      await clientStream.readable.cancel(new Error('some error'));

      // Canceled readable will just complete
      await consumeReadable(clientStream);
      // TODO: proper error
      await expect(consumeReadable(serverStream)).rejects.toThrow();

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    test('should fully complete with FRC, RRC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      await serverStream.readable.cancel(new Error('some error'));
      await clientStream.readable.cancel(new Error('some error'));

      // Canceled readable will just complete
      await consumeReadable(clientStream);
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
    test('should fully complete with FWA, FRC, RWA, RRC', async () => {
      const serverStreamP = firstValueFrom(serverConnection.stream$);
      const clientStream = clientConnection.newStream();
      const clientWriter = clientStream.writable.getWriter();
      // Writing a message should trigger the stream creation on the server side.
      await clientWriter.write(Buffer.from('message'));
      const serverStream = await serverStreamP;
      const serverWriter = serverStream.writable.getWriter();
      await Promise.all([
        clientWriter.abort(new Error('some error')),
        serverStream.readable.cancel(new Error('some error')),
        serverWriter.abort(new Error('some error')),
        clientStream.readable.cancel(new Error('some error')),
      ]);

      // Canceled readable will just complete
      await consumeReadable(clientStream);
      await consumeReadable(serverStream);

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
  });

  test.todo('should clean up streams if connection times out');
  test.todo('streams should contain metadata');
  test.todo('connection can be forced closed after unforced close');
  test.todo(
    'connections handle packets failing to send and gracefully timeout',
  );
  test.todo('should throw stream limit error when limit is reached');
  test.todo('ended streams do not contribute to limit');
});
