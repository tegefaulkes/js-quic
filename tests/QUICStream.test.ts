import type {
  ClientCryptoOps,
  ServerCryptoOps,
  StreamCodeToReason,
  StreamReasonToCode,
} from '#types.js';
import type { TLSConfigs } from './utils.js';
import type QUICConnection from '#QUICConnection.js';
import type { Subject } from 'rxjs';
import Logger, { formatting, LogLevel, StreamHandler } from '@matrixai/logger';
import { test } from '@fast-check/jest';
import { firstValueFrom } from 'rxjs';
import { sleep } from './utils.js';
import * as testsUtils from './utils.js';
import { generateTLSConfig } from './utils.js';
import QUICServer from '#QUICServer.js';
import QUICClient from '#QUICClient.js';
import QUICStream from '#QUICStream.js';
import * as utils from '#utils.js';
import * as errors from '#errors.js';

async function consumeReadable(stream: QUICStream) {
  for await (const _chunk of stream.readable) {
    // Do nothing, only consume to completion
  }
}

describe('QUICStream', () => {
  const _logger = new Logger(`QUICStream Test`, LogLevel.WARN, [
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

  let serverTlsConfig: TLSConfigs;
  let server: QUICServer;
  let serverConnection: QUICConnection;
  let clientTlsConfig: TLSConfigs;
  let client: QUICClient;
  let clientConnection: QUICConnection;

  const reasonSymbol = Symbol('reasonSymbol');
  const codeToReason: StreamCodeToReason = (type, code) => {
    if (code === 100) return reasonSymbol;
    else return new Error(`${type.toString()} ${code.toString()}`);
  };
  const reasonToCode: StreamReasonToCode = (_type, reason) => {
    if (reason === reasonSymbol) return 100;
    else return 1;
  };

  // We need to test the stream-making
  beforeEach(async () => {
    key = await testsUtils.generateKeyHMAC();
    socketCleanMethods = testsUtils.socketCleanupFactory();

    serverTlsConfig = await generateTLSConfig(defaultType);
    clientTlsConfig = await generateTLSConfig(defaultType);
    server = new QUICServer({
      crypto: {
        key,
        ops: serverCrypto,
      },
      codeToReason,
      reasonToCode,
      logger: loggerServer.getChild(QUICServer.name),
      config: {
        key: serverTlsConfig.leafKeyPairPEM.privateKey,
        cert: serverTlsConfig.leafCertPEM,
        ca: clientTlsConfig.caCertPEM,
        verifyPeer: true,
        maxIdleTimeout: 1000,
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
      codeToReason,
      reasonToCode,
      logger: loggerClient.getChild(QUICClient.name),
      config: {
        key: clientTlsConfig.leafKeyPairPEM.privateKey,
        cert: clientTlsConfig.leafCertPEM,
        ca: serverTlsConfig.caCertPEM,
        verifyPeer: true,
        maxIdleTimeout: 1000,
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
    await expect(readP).rejects.toThrow('read 1');
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
    await expect(readP).rejects.toThrow('read 1');
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
    await expect(clientWriter.write(message)).rejects.toThrow(`write 1`);
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

    await expect(consumeReadable(serverStream)).rejects.toThrow(
      errors.ErrorQUICStreamKilled,
    );
    await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');

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

    await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');

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

    await expect(consumeReadable(serverStream)).rejects.toThrow(
      errors.ErrorQUICStreamKilled,
    );

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

    await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');

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

    await expect(consumeReadable(serverStream)).rejects.toThrow(
      errors.ErrorQUICStreamKilled,
    );
    await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');

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
        try {
          for await (const _chunk of stream.readable) {
            // Just consume the stream data
          }
        } catch {
          // Ignore error
        }

        await firstValueFrom(stream.complete$);
      }),
    );
    const endingStreamsP = clientConnection.endStreams(false);
    await streamsCloseP;
    await endingStreamsP;

    expect(() => clientConnection.newStream()).toThrow(
      errors.ErrorQUICConnectionDraining,
    );
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
      await expect(readP).rejects.toThrow('read 1');
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
      await expect(clientWriter.write(Buffer.from('message'))).rejects.toThrow(
        'write 1',
      );
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
      await expect(consumeReadable(serverStream)).rejects.toThrow('read 1');

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

      await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');
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

      await expect(clientWriter.write(Buffer.from('message'))).rejects.toThrow(
        'write 1',
      );
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

      await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');
      await expect(consumeReadable(serverStream)).rejects.toThrow('read 1');

      await firstValueFrom(clientStream.writableComplete$);
      await firstValueFrom(clientStream.readableComplete$);
      await firstValueFrom(clientStream.complete$);
      await firstValueFrom(serverStream.writableComplete$);
      await firstValueFrom(serverStream.readableComplete$);
      await firstValueFrom(serverStream.complete$);
    });
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

      await expect(consumeReadable(clientStream)).rejects.toThrow('read 1');
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
      await expect(consumeReadable(serverStream)).rejects.toThrow('read 1');

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
  test('stream completion should complete all observables', async () => {
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

    async function completionProm(subject: Subject<unknown>): Promise<void> {
      const { resolveP, p } = utils.promise<void>();
      subject.subscribe({
        complete: () => {
          resolveP();
        },
      });
      return p;
    }

    await completionProm(clientStream.readReady$);
    await completionProm(clientStream.writeReady$);
    await completionProm(clientStream.streamEvents$);
    await completionProm(clientStream.finished$);
    await completionProm(clientStream.readableComplete$);
    await completionProm(clientStream.writableComplete$);
    await completionProm(clientStream.complete$);

    await completionProm(serverStream.readReady$);
    await completionProm(serverStream.writeReady$);
    await completionProm(serverStream.streamEvents$);
    await completionProm(serverStream.finished$);
    await completionProm(serverStream.readableComplete$);
    await completionProm(serverStream.writableComplete$);
    await completionProm(serverStream.complete$);
  });
  test('should clean up streams if connection times out', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    // Force packet loss
    serverConnection.send$.complete();
    clientConnection.send$.complete();

    await firstValueFrom(serverConnection.timedOut$);
    await firstValueFrom(clientConnection.timedOut$);

    await expect(consumeReadable(serverStream)).rejects.toThrow(
      errors.ErrorQUICConnectionIdleTimeout,
    );
    await expect(consumeReadable(clientStream)).rejects.toThrow(
      errors.ErrorQUICConnectionIdleTimeout,
    );

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('should clean up streams if connection closes without warning', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    // Force connection close
    clientConnection.kill({
      isApp: true,
      errorCode: 1,
      reason: Buffer.from('some reason'),
    });

    await firstValueFrom(serverConnection.closed$);
    await firstValueFrom(clientConnection.closed$);

    await expect(consumeReadable(serverStream)).rejects.toThrow(
      errors.ErrorQUICConnectionClosed,
    );
    await expect(consumeReadable(clientStream)).rejects.toThrow(
      errors.ErrorQUICConnectionClosed,
    );

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('streams should contain metadata', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;

    expect(clientStream.sourceHost).toBe(localhost);
    expect(clientStream.sourcePort).toBe(serverConnection.port);
    expect(clientStream.host).toBe(localhost);
    expect(clientStream.port).toBe(serverConnection.sourcePort);
    expect(utils.derToPEM(clientStream.remoteCertChain![0])).toBe(
      serverTlsConfig.leafCertPEM,
    );

    await firstValueFrom(serverConnection.peerCertChain$);
    expect(serverStream.sourceHost).toBe(localhost);
    expect(serverStream.sourcePort).toBe(clientConnection.port);
    expect(serverStream.host).toBe(localhost);
    expect(serverStream.port).toBe(clientConnection.sourcePort);
    expect(utils.derToPEM(serverStream.remoteCertChain![0])).toBe(
      clientTlsConfig.leafCertPEM,
    );
  });
  test('connection can be forced closed after unforced close', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;

    const endStreamsP = clientConnection.endStreams(false);
    await sleep(100);
    expect(clientStream.isComplete).toBeFalse();
    expect(serverStream.isComplete).toBeFalse();
    void clientConnection.endStreams(true);
    await endStreamsP;

    await firstValueFrom(serverConnection.timedOut$);
    await firstValueFrom(clientConnection.timedOut$);

    await expect(consumeReadable(serverStream)).rejects.toThrow('read 1');
    await expect(consumeReadable(clientStream)).rejects.toThrow(
      errors.ErrorQUICStreamKilled,
    );

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  test('should throw stream limit error when limit is reached', async () => {
    // Stream limit is 100
    for (let i = 0; i < 100; i++) {
      clientConnection.newStream();
    }
    expect(() => clientConnection.newStream()).toThrow(
      errors.ErrorQUICStreamLimit,
    );
  });
  test('ended streams do not contribute to limit', async () => {
    // Stream limit is 100
    const serverStreams: Array<Promise<void>> = [];
    serverConnection.stream$.subscribe((stream) => {
      serverStreams.push(firstValueFrom(stream.complete$));
    });
    const clientStreams: Array<Promise<void>> = [];
    const { p: waitP, resolveP } = utils.promise<void>();
    for (let i = 0; i < 100; i++) {
      const clientStream = clientConnection.newStream();
      clientStreams.push(
        (async () => {
          await waitP;
          clientStream.kill();
          await firstValueFrom(clientStream.complete$);
        })(),
      );
    }

    resolveP();
    expect(clientConnection.connection.peerStreamsLeftBidi()).toBe(0);
    await Promise.all(clientStreams);
    await Promise.all(serverStreams);
    // Wait for streams to be cleaned up
    await sleep(100);
    expect(clientConnection.connection.peerStreamsLeftBidi()).toBe(100);
  });
  test('test custom code to reason', async () => {
    const serverStreamP = firstValueFrom(serverConnection.stream$);
    const clientStream = clientConnection.newStream();
    const clientWriter = clientStream.writable.getWriter();
    // Writing a message should trigger the stream creation on the server side.
    await clientWriter.write(Buffer.from('message'));
    const serverStream = await serverStreamP;
    // @ts-ignore: using a symbol for the test here
    serverStream.kill(reasonSymbol);

    await expect(consumeReadable(serverStream)).rejects.toBe(
      // @ts-ignore: using a symbol for the test here
      reasonSymbol,
    );
    await expect(consumeReadable(clientStream)).rejects.toBe(
      // @ts-ignore: using a symbol for the test here
      reasonSymbol,
    );

    await firstValueFrom(clientStream.writableComplete$);
    await firstValueFrom(clientStream.readableComplete$);
    await firstValueFrom(clientStream.complete$);
    await firstValueFrom(serverStream.writableComplete$);
    await firstValueFrom(serverStream.readableComplete$);
    await firstValueFrom(serverStream.complete$);
  });
  // TODO: implement uni stream handling.
  //  The only difference is one of the streams starts closed when the stream is created.
  test.todo('test unidirectional streams');
});
