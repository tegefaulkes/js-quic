import type { ClientCryptoOps, ServerCryptoOps } from '#types.js';
import type { KeyTypes, TLSConfigs } from './utils.js';
import Logger, { LogLevel, StreamHandler, formatting } from '@matrixai/logger';
import { test } from '@fast-check/jest';
import { firstValueFrom, Subject, timer } from 'rxjs';
import * as fc from 'fast-check';
import * as testsUtils from './utils.js';
import { sleep } from './utils.js';
import QUICClient from '#QUICClient.js';
import QUICServer from '#QUICServer.js';
import * as errors from '#errors.js';
import * as events from '#events.js';
import QUICSocket from '#QUICSocket.js';
import * as utils from '#utils.js';

describe(QUICClient.name, () => {
  const logger = new Logger(`${QUICClient.name} Test`, LogLevel.INFO, [
    new StreamHandler(
      formatting.format`${formatting.level}:${formatting.keys}:${formatting.msg}`,
    ),
  ]);
  const localhost = '127.0.0.1';
  // Intentional hard-coded port, no destination exists
  const noTargetPort = 55544;
  // This has to be set up asynchronously due to key generation
  const serverCryptoOps: ServerCryptoOps = {
    sign: testsUtils.signHMAC,
    verify: testsUtils.verifyHMAC,
  };
  let key: ArrayBuffer;
  const clientCryptoOps: ClientCryptoOps = {
    randomBytes: testsUtils.randomBytes,
  };
  let socketCleanMethods: ReturnType<typeof testsUtils.socketCleanupFactory>;

  const types: Array<KeyTypes> = ['RSA', 'ECDSA', 'Ed25519'];
  const defaultType = types[0];
  const clientCrypto: ClientCryptoOps = {
    randomBytes: testsUtils.randomBytes,
  };

  // We need to test making streams
  beforeEach(async () => {
    key = await testsUtils.generateKeyHMAC();
    socketCleanMethods = testsUtils.socketCleanupFactory();
  });
  afterEach(async () => {
    await socketCleanMethods.stopSockets();
  });
  // Are we describing a dual stack client!?
  describe('dual stack client', () => {
    test('to ipv4 server succeeds', async () => {
      const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);

      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigServer.leafKeyPairPEM.privateKey,
          cert: tlsConfigServer.leafCertPEM,
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
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      const connection = await connectionP;
      if (connection == null) throw new Error('connection not found');
      expect(connection.sourceHost).toBe('127.0.0.1');
      expect(connection.sourcePort).toBe(server.port);
      expect(connection.host).toBe('127.0.0.1');
      expect(connection.port).toBe(client.localPort);
      await sleep(2000);
      await firstValueFrom(connection.established$);
      await firstValueFrom(client.connection.established$);
      logger.warn('killing!');
      await client.destroy();
      await server.stop();
    });
    test('to ipv6 server succeeds', async () => {
      const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigServer.leafKeyPairPEM.privateKey,
          cert: tlsConfigServer.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(server);
      const connectionP = firstValueFrom(server.connection$, {
        defaultValue: undefined,
      });

      await server.start({
        host: '::1',
      });
      const client = await QUICClient.createQUICClient({
        host: '::1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      const connection = await connectionP;
      if (connection == null) throw Error('connection not found');
      expect(connection.sourceHost).toBe('::1');
      expect(connection.sourcePort).toBe(server.port);
      expect(connection.host).toBe('::1');
      expect(connection.port).toBe(client.localPort);
      await client.destroy();
      await server.stop();
    });
    test('to dual stack server succeeds', async () => {
      const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigServer.leafKeyPairPEM.privateKey,
          cert: tlsConfigServer.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(server);
      const connectionP = firstValueFrom(server.connection$, {
        defaultValue: undefined,
      });
      await server.start({
        host: '::',
      });
      const client = await QUICClient.createQUICClient({
        host: '::', // Will resolve to ::1
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      const connection = await connectionP;
      if (connection == null) throw Error('connection not found');
      expect(connection.sourceHost).toBe('::');
      expect(connection.sourcePort).toBe(server.port);
      expect(connection.host).toBe('::1');
      expect(connection.port).toBe(client.localPort);
      await client.destroy();
      await server.stop();
    });
  });
  describe('hard connection failures', () => {
    test('times out with maxIdleTimeout when there is no server', async () => {
      // QUICClient repeatedly dials until the connection timeout
      await expect(
        QUICClient.createQUICClient({
          host: localhost,
          port: noTargetPort,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            maxIdleTimeout: 200,
            verifyPeer: false,
          },
        }),
      ).rejects.toThrow(errors.ErrorQUICConnectionIdleTimeout);
    });
    test('intervalTimeoutTime must be less than maxIdleTimeout', async () => {
      // Larger keepAliveIntervalTime throws
      await expect(
        QUICClient.createQUICClient({
          host: localhost,
          port: noTargetPort,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            maxIdleTimeout: 200,
            keepAliveIntervalTime: 1000,
            verifyPeer: false,
          },
        }),
      ).rejects.toThrow(errors.ErrorQUICConnectionConfigInvalid);
      // Smaller keepAliveIntervalTime doesn't cause a problem
      await expect(
        QUICClient.createQUICClient({
          host: localhost,
          port: noTargetPort,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            maxIdleTimeout: 200,
            keepAliveIntervalTime: 100,
            verifyPeer: false,
          },
        }),
      ).rejects.not.toThrow(errors.ErrorQUICConnectionConfigInvalid);
      // Not setting an interval doesn't cause a problem either
      await expect(
        QUICClient.createQUICClient({
          host: localhost,
          port: noTargetPort,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            maxIdleTimeout: 200,
            verifyPeer: false,
          },
        }),
      ).rejects.not.toThrow(errors.ErrorQUICConnectionConfigInvalid);
    });
    test('client times out with ctx timer while starting', async () => {
      // QUICClient repeatedly dials until the connection timeout
      await expect(
        QUICClient.createQUICClient(
          {
            host: localhost,
            port: noTargetPort,
            localHost: localhost,
            crypto: {
              ops: clientCryptoOps,
            },
            logger: logger.getChild(QUICClient.name),
            config: {
              // Prevent `maxIdleTimeout` timeout
              maxIdleTimeout: 100000,
              verifyPeer: false,
            },
          },
          timer(100),
        ),
      ).rejects.toThrow(errors.ErrorQUICClientAborted);
    });
    test('client times out with abort while starting', async () => {
      // QUICClient repeatedly dials until the connection timeout
      const abort = new Subject<void>();
      const clientProm = QUICClient.createQUICClient(
        {
          host: localhost,
          port: noTargetPort,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            // Prevent `maxIdleTimeout` timeout
            maxIdleTimeout: 100000,
            verifyPeer: false,
          },
        },
        abort,
      );
      await sleep(100);
      abort.next();
      await expect(clientProm).rejects.toThrow(errors.ErrorQUICClientAborted);
    });
  });

  describe.each(types)('TLS rotation with %s', (type) => {
    test('existing connections config is unchanged and still function', async () => {
      const tlsConfig1 = await testsUtils.generateTLSConfig(type);
      const tlsConfig2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig1.leafKeyPairPEM.privateKey,
          cert: tlsConfig1.leafCertPEM,
        },
      });
      socketCleanMethods.extractSocket(server);
      await server.start({
        host: localhost,
      });
      const client1 = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: true,
          verifyAllowFail: true,
        },
      });
      socketCleanMethods.extractSocket(client1);
      const peerCertChainInitial = client1.connection.peerCertChain;
      server.updateConfig({
        key: tlsConfig2.leafKeyPairPEM.privateKey,
        cert: tlsConfig2.leafCertPEM,
      });
      // The existing connection's certs should be unchanged
      const peerCertChainNew = client1.connection.peerCertChain;
      expect(peerCertChainNew![0].toString()).toStrictEqual(
        peerCertChainInitial![0].toString(),
      );
      await client1.destroy();
      await server.stop();
    });
    test('new connections use new config', async () => {
      const tlsConfig1 = await testsUtils.generateTLSConfig(type);
      const tlsConfig2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig1.leafKeyPairPEM.privateKey,
          cert: tlsConfig1.leafCertPEM,
        },
      });
      socketCleanMethods.extractSocket(server);
      await server.start({
        host: localhost,
      });
      const client1 = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: true,
          verifyAllowFail: true,
        },
      });
      socketCleanMethods.extractSocket(client1);
      const peerCertChainInitial = client1.connection.peerCertChain;
      server.updateConfig({
        key: tlsConfig2.leafKeyPairPEM.privateKey,
        cert: tlsConfig2.leafCertPEM,
      });
      // Starting a new connection has a different peerCertChain
      const client2 = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: true,
          verifyAllowFail: true,
        },
      });
      socketCleanMethods.extractSocket(client2);
      const peerCertChainNew = client2.connection.peerCertChain;
      expect(peerCertChainNew![0].toString()).not.toStrictEqual(
        peerCertChainInitial![0].toString(),
      );
      await client1.destroy();
      await client2.destroy();
      await server.stop();
    });
  });

  describe.each(types)('graceful tls handshake with %s certs', (type) => {
    test('server verification succeeds', async () => {
      const tlsConfigs = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigs.leafKeyPairPEM.privateKey,
          cert: tlsConfigs.leafCertPEM,
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
      // Connection should succeed
      const client = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: true,
          ca: tlsConfigs.caCertPEM,
        },
      });
      socketCleanMethods.extractSocket(client);
      await connectionP;
      await client.destroy();
      await server.stop();
    });
    test('client verification succeeds', async () => {
      const tlsConfigs1 = await testsUtils.generateTLSConfig(type);
      const tlsConfigs2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigs1.leafKeyPairPEM.privateKey,
          cert: tlsConfigs1.leafCertPEM,
          verifyPeer: true,
          ca: tlsConfigs2.caCertPEM,
        },
      });
      const connectionP = firstValueFrom(server.connection$, {
        defaultValue: undefined,
      });
      await server.start({
        host: localhost,
      });
      // Connection should succeed
      const client = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          key: tlsConfigs2.leafKeyPairPEM.privateKey,
          cert: tlsConfigs2.leafCertPEM,
          verifyPeer: false,
        },
      });
      await connectionP;
      await client.destroy();
      await server.stop();
    });
    test('client and server verification succeeds', async () => {
      const tlsConfigs1 = await testsUtils.generateTLSConfig(type);
      const tlsConfigs2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigs1.leafKeyPairPEM.privateKey,
          cert: tlsConfigs1.leafCertPEM,
          ca: tlsConfigs2.caCertPEM,
          verifyPeer: true,
        },
      });
      socketCleanMethods.extractSocket(server);
      const connectionP = firstValueFrom(server.connection$, {
        defaultValue: undefined,
      });
      await server.start({
        host: localhost,
      });
      // Connection should succeed
      const client = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          key: tlsConfigs2.leafKeyPairPEM.privateKey,
          cert: tlsConfigs2.leafCertPEM,
          ca: tlsConfigs1.caCertPEM,
          verifyPeer: true,
        },
      });
      socketCleanMethods.extractSocket(client);
      await connectionP;
      await client.destroy();
      await server.stop();
    });
    test('graceful failure verifying server', async () => {
      const tlsConfigs1 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigs1.leafKeyPairPEM.privateKey,
          cert: tlsConfigs1.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(server);
      await server.start({
        host: localhost,
      });
      // Connection should fail
      await expect(
        QUICClient.createQUICClient({
          host: localhost,
          port: server.port,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            verifyPeer: true,
          },
        }),
      ).toReject();
      await server.stop();
    });
    test('graceful failure verifying client', async () => {
      const tlsConfigs1 = await testsUtils.generateTLSConfig(type);
      const tlsConfigs2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigs1.leafKeyPairPEM.privateKey,
          cert: tlsConfigs1.leafCertPEM,
          verifyPeer: true,
        },
      });
      socketCleanMethods.extractSocket(server);
      await server.start({
        host: localhost,
      });
      // Connection succeeds but peer will reject shortly after
      const client = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          key: tlsConfigs2.leafKeyPairPEM.privateKey,
          cert: tlsConfigs2.leafCertPEM,
          verifyPeer: false,
        },
      });
      // Verification by peer happens after connection is securely established and started
      const errorP = firstValueFrom(client.connection.error$, {
        defaultValue: undefined,
      });
      // Expect an error event
      await errorP;
      await server.stop();
      await client.destroy();
    });
    test('graceful failure verifying client and server', async () => {
      const tlsConfigs1 = await testsUtils.generateTLSConfig(type);
      const tlsConfigs2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfigs1.leafKeyPairPEM.privateKey,
          cert: tlsConfigs1.leafCertPEM,
          verifyPeer: true,
        },
      });
      socketCleanMethods.extractSocket(server);
      await server.start({
        host: localhost,
      });
      // Connection should fail
      await expect(
        QUICClient.createQUICClient({
          host: localhost,
          port: server.port,
          localHost: localhost,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            key: tlsConfigs2.leafKeyPairPEM.privateKey,
            cert: tlsConfigs2.leafCertPEM,
            verifyPeer: true,
          },
        }),
      ).rejects.toThrow(errors.ErrorQUICConnectionLocalTLS);
      await server.stop();
    });
  });

  // Todo: implement proper feedback for failed sends on a connection
  test.skip('invalid arguments causes `createQUICClient` to fail', async () => {
    await expect(
      QUICClient.createQUICClient({
        host: '123.123.123.123', // Invalid ip when bound to loopback
        port: 55555,
        localHost: localhost,
        crypto: {
          ops: clientCrypto,
        },
        config: {
          verifyPeer: false,
        },
        logger: logger.getChild(QUICClient.name),
      }),
    ).rejects.toThrow(errors.ErrorQUICClientInvalidArgument);
  });

  describe('handles random packets', () => {
    test.prop(
      [
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
      ],
      { numRuns: 1 },
    )('client handles random noise from server', async (data, messages) => {
      const tlsConfig = await testsUtils.generateTLSConfig('RSA');
      const socket = new QUICSocket({
        logger: logger.getChild('socket'),
      });
      await socket.start({
        host: localhost,
      });
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
        },
        socket,
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      let droppedPacketCount = 0;
      // @ts-ignore: using protected property
      client.socket.quicSocketMessageDropped$.subscribe(
        () => droppedPacketCount++,
      );
      const serverConnection = await serverConnectionP;
      // Do the test
      const serverStreamProms: Array<Promise<void>> = [];
      serverConnection?.stream$.subscribe((stream) => {
        const streamProm = stream.readable.pipeTo(stream.writable);
        serverStreamProms.push(streamProm);
      });
      // Sending random data to client from the perspective of the server
      let running = true;
      let badPacketsSent = 0;
      const randomDataProm = (async () => {
        while (running) {
          await socket.send(
            data[badPacketsSent % data.length],
            client.localPort,
            '127.0.0.1',
          );
          await sleep(5);
          badPacketsSent += 1;
        }
      })();
      // We want to check that things function fine between bad data
      const randomActivityProm = (async () => {
        const stream = client.connection.newStream();
        await Promise.all([
          (async () => {
            // Write data
            const writer = stream.writable.getWriter();
            for (const message of messages) {
              await writer.write(Buffer.from(message));
              await sleep(7);
            }
            await writer.close();
          })(),
          (async () => {
            // Consume readable
            for await (const _ of stream.readable) {
              // Do nothing
            }
          })(),
        ]);
        running = false;
      })();
      // Wait for running activity to finish, should complete without error
      await Promise.all([
        randomActivityProm,
        serverStreamProms,
        randomDataProm,
      ]);
      expect(droppedPacketCount).toBe(badPacketsSent);
      await client.destroy({ force: true });
      await server.stop({ force: true });
      await socket.stop();
    });

    test.prop(
      [
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
      ],
      { numRuns: 1 },
    )('client handles random noise from external', async (data, messages) => {
      const tlsConfig = await testsUtils.generateTLSConfig('RSA');
      const socket = new QUICSocket({
        logger: logger.getChild('socket'),
      });
      await socket.start({
        host: localhost,
      });
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      let droppedPacketCount = 0;
      // @ts-ignore: using protected property
      client.socket.quicSocketMessageDropped$.subscribe(
        () => droppedPacketCount++,
      );
      const serverConnection = await serverConnectionP;
      // Do the test
      const serverStreamProms: Array<Promise<void>> = [];
      serverConnection.stream$.subscribe((stream) => {
        const streamProm = stream.readable.pipeTo(stream.writable);
        serverStreamProms.push(streamProm);
      });
      // Sending random data to client from the perspective of the server
      let running = true;
      let badPacketsSent = 0;
      const randomDataProm = (async () => {
        while (running) {
          await socket.send(
            data[badPacketsSent % data.length],
            client.localPort,
            '127.0.0.1',
          );
          await sleep(5);
          badPacketsSent += 1;
        }
      })();
      // We want to check that things function fine between bad data
      const randomActivityProm = (async () => {
        const stream = client.connection.newStream();
        await Promise.all([
          (async () => {
            // Write data
            const writer = stream.writable.getWriter();
            for (const message of messages) {
              await writer.write(Buffer.from(message));
              await sleep(7);
            }
            await writer.close();
          })(),
          (async () => {
            // Consume readable
            for await (const _ of stream.readable) {
              // Do nothing
            }
          })(),
        ]);
        running = false;
      })();
      // Wait for running activity to finish, should complete without error
      await Promise.all([
        randomActivityProm,
        serverStreamProms,
        randomDataProm,
      ]);
      expect(droppedPacketCount).toBe(badPacketsSent);
      await client.destroy({ force: true });
      await server.stop();
      await socket.stop();
    });

    test.prop(
      [
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
      ],
      { numRuns: 1 },
    )('server handles random noise from client', async (data, messages) => {
      const tlsConfig = await testsUtils.generateTLSConfig('RSA');
      const socket = new QUICSocket({
        logger: logger.getChild('socket'),
      });
      await socket.start({
        host: localhost,
      });
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(server);
      let droppedPacketCount = 0;
      // @ts-ignore: using protected property
      server.socket.quicSocketMessageDropped$.subscribe(
        () => droppedPacketCount++,
      );
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        socket,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      const serverConnection = await serverConnectionP;
      // Do the test
      const serverStreamProms: Array<Promise<void>> = [];
      serverConnection.stream$.subscribe((stream) => {
        const streamProm = stream.readable.pipeTo(stream.writable);
        serverStreamProms.push(streamProm);
      });
      // Sending random data to client from the perspective of the server
      let running = true;
      let badPacketsSent = 0;
      const randomDataProm = (async () => {
        while (running) {
          await socket.send(
            data[badPacketsSent % data.length],
            server.port,
            '127.0.0.1',
          );
          await sleep(5);
          badPacketsSent += 1;
        }
      })();
      // We want to check that things function fine between bad data
      const randomActivityProm = (async () => {
        const stream = client.connection.newStream();
        await Promise.all([
          (async () => {
            // Write data
            const writer = stream.writable.getWriter();
            for (const message of messages) {
              await writer.write(Buffer.from(message));
              await sleep(7);
            }
            await writer.close();
          })(),
          (async () => {
            // Consume readable
            for await (const _ of stream.readable) {
              // Do nothing
            }
          })(),
        ]);
        running = false;
      })();
      // Wait for running activity to finish, should complete without error
      await Promise.all([
        randomActivityProm,
        serverStreamProms,
        randomDataProm,
      ]);
      expect(droppedPacketCount).toBe(badPacketsSent);
      await client.destroy({ force: true });
      await server.stop({ force: true });
      await socket.stop();
    });

    test.prop(
      [
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
        fc.noShrink(
          fc.array(fc.uint8Array({ minLength: 1 }), { minLength: 5 }),
        ),
      ],
      { numRuns: 1 },
    )('server handles random noise from external', async (data, messages) => {
      const tlsConfig = await testsUtils.generateTLSConfig('RSA');
      const socket = new QUICSocket({
        logger: logger.getChild('socket'),
      });
      await socket.start({
        host: localhost,
      });
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(server);
      let droppedPacketCount = 0;
      // @ts-ignore: using protected property
      server.socket.quicSocketMessageDropped$.subscribe(
        () => droppedPacketCount++,
      );
      const connectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client);
      const serverConnection = await connectionP;
      // Do the test
      const serverStreamProms: Array<Promise<void>> = [];
      serverConnection.stream$.subscribe((stream) => {
        const streamProm = stream.readable.pipeTo(stream.writable);
        serverStreamProms.push(streamProm);
      });
      // Sending random data to client from the perspective of the server
      let running = true;
      let badPacketsSent = 0;
      const randomDataProm = (async () => {
        while (running) {
          await socket.send(
            data[badPacketsSent % data.length],
            server.port,
            '127.0.0.1',
          );
          await sleep(5);
          badPacketsSent += 1;
        }
      })();
      // We want to check that things function fine between bad data
      const randomActivityProm = (async () => {
        const stream = client.connection.newStream();
        await Promise.all([
          (async () => {
            // Write data
            const writer = stream.writable.getWriter();
            for (const message of messages) {
              await writer.write(Buffer.from(message));
              await sleep(7);
            }
            await writer.close();
          })(),
          (async () => {
            // Consume readable
            for await (const _ of stream.readable) {
              // Do nothing
            }
          })(),
        ]);
        running = false;
      })();
      // Wait for running activity to finish, should complete without error
      await Promise.all([
        randomActivityProm,
        serverStreamProms,
        randomDataProm,
      ]);
      expect(droppedPacketCount).toBe(badPacketsSent);
      await client.destroy({ force: true });
      await server.stop();
      await socket.stop();
    });
  });

  describe('keepalive', () => {
    let tlsConfig: TLSConfigs;
    beforeEach(async () => {
      tlsConfig = await testsUtils.generateTLSConfig('RSA');
    });
    test('connection can time out on client', async () => {
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
          maxIdleTimeout: 1000,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
          maxIdleTimeout: 100,
        },
      });
      socketCleanMethods.extractSocket(client);
      // Setting no keepalive should cause the connection to time out
      // It has cleaned up due to timeout
      const clientConnection = client.connection;
      await firstValueFrom(clientConnection.timedOut$);
      const serverConnection = await serverConnectionP;
      await sleep(100);
      // Server and client has cleaned up

      expect(clientConnection.closed).toBeTrue();
      expect(serverConnection.closed).toBeTrue();

      await client.destroy();
      await server.stop();
    });
    test('connection can time out on server', async () => {
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
          maxIdleTimeout: 100,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
          maxIdleTimeout: 1000,
        },
      });
      socketCleanMethods.extractSocket(client);
      // Setting no keepalive should cause the connection to time out
      // It has cleaned up due to timeout
      const clientConnection = client.connection;
      const serverConnection = await serverConnectionP;
      await firstValueFrom(serverConnection.timedOut$);
      await sleep(100);
      // Server and client has cleaned up
      expect(clientConnection.closed).toBeTrue();
      expect(serverConnection.closed).toBeTrue();

      await client.destroy();
      await server.stop();
    });
    test('keep alive prevents timeout on client', async () => {
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
          maxIdleTimeout: 20000,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
          maxIdleTimeout: 100,
          keepAliveIntervalTime: 50,
        },
      });
      socketCleanMethods.extractSocket(client);
      // Setting no keepalive should cause the connection to time out
      // It has cleaned up due to timeout
      const clientConnection = client.connection;
      const clientTimeoutP = firstValueFrom(clientConnection.timedOut$);
      await serverConnectionP;
      // Connection would time out after 100ms if keep alive didn't work
      await Promise.race([
        sleep(300),
        clientTimeoutP.then(() => {
          throw Error('Connection timed out');
        }),
      ]);
      await client.destroy();
      await server.stop();
    });
    test('keep alive prevents timeout on server', async () => {
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
          maxIdleTimeout: 100,
          keepAliveIntervalTime: 50,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
          maxIdleTimeout: 20000,
        },
      });
      socketCleanMethods.extractSocket(client);
      // Setting no keepalive should cause the connection to time out
      // It has cleaned up due to timeout
      const serverConnection = await serverConnectionP;
      const serverTimeoutP = firstValueFrom(serverConnection.timedOut$);
      // Connection would time out after 100ms if keep alive didn't work
      await Promise.race([
        sleep(300),
        serverTimeoutP.then(() => {
          throw Error('Connection timed out');
        }),
      ]);
      await client.destroy();
      await server.stop();
    });
    test('client keep alive prevents timeout on server', async () => {
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig.leafKeyPairPEM.privateKey,
          cert: tlsConfig.leafCertPEM,
          verifyPeer: false,
          maxIdleTimeout: 100,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      const client = await QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: server.port,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
          maxIdleTimeout: 20000,
          keepAliveIntervalTime: 50,
        },
      });
      socketCleanMethods.extractSocket(client);
      // Setting no keepalive should cause the connection to time out
      // It has cleaned up due to timeout
      const serverConnection = await serverConnectionP;
      const serverTimeoutP = firstValueFrom(serverConnection.timedOut$);
      // Connection would time out after 100ms if keep alive didn't work
      await Promise.race([
        sleep(300),
        serverTimeoutP.then(() => {
          throw Error('Connection timed out');
        }),
      ]);
      await client.destroy();
      await server.stop();
    });
    test('Keep alive does not prevent connection timeout', async () => {
      const clientProm = QUICClient.createQUICClient({
        host: '::ffff:127.0.0.1',
        port: noTargetPort,
        localHost: '::',
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
          maxIdleTimeout: 100,
          keepAliveIntervalTime: 50,
        },
      });
      await expect(clientProm).rejects.toThrow(
        errors.ErrorQUICConnectionIdleTimeout,
      );
    });
  });

  test('connections are established and secured quickly', async () => {
    const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);
    const server = new QUICServer({
      crypto: {
        key,
        ops: serverCryptoOps,
      },
      logger: logger.getChild(QUICServer.name),
      config: {
        key: tlsConfigServer.leafKeyPairPEM.privateKey,
        cert: tlsConfigServer.leafCertPEM,
        verifyPeer: false,
      },
    });
    socketCleanMethods.extractSocket(server);
    const serverConnectionP = firstValueFrom(server.connection$);
    await server.start({
      host: localhost,
    });
    // If the server is slow to respond then this will time out.
    //  Then main cause of this was the server not processing the initial packet
    //  that creates the `QUICConnection`, as a result, the whole creation waited
    //  an extra 1 second for the client to retry the initial packet.
    const client = await QUICClient.createQUICClient(
      {
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      },
      timer(500),
    );
    socketCleanMethods.extractSocket(client);
    await serverConnectionP;
    await client.destroy({ force: true });
    await server.stop({ force: true });
  });

  test('socket stopping first triggers client destruction', async () => {
    const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);

    const server = new QUICServer({
      crypto: {
        key,
        ops: serverCryptoOps,
      },
      logger: logger.getChild(QUICServer.name),
      config: {
        key: tlsConfigServer.leafKeyPairPEM.privateKey,
        cert: tlsConfigServer.leafCertPEM,
        verifyPeer: false,
        maxIdleTimeout: 200,
      },
    });
    socketCleanMethods.extractSocket(server);
    const serverConnectionP = firstValueFrom(server.connection$);
    await server.start({
      host: localhost,
    });
    // If the server is slow to respond then this will time out.
    //  Then main cause of this was the server not processing the initial packet
    //  that creates the `QUICConnection`, as a result, the whole creation waited
    //  an extra 1 second for the client to retry the initial packet.
    const client = await QUICClient.createQUICClient({
      host: localhost,
      port: server.port,
      localHost: localhost,
      crypto: {
        ops: clientCryptoOps,
      },
      logger: logger.getChild(QUICClient.name),
      config: {
        verifyPeer: false,
      },
    });
    socketCleanMethods.extractSocket(client);

    const serverConnection = await serverConnectionP;

    const clientConnectionErrorP = firstValueFrom(client.connection.error$);
    const clientConnectionClosedP = firstValueFrom(client.connection.closed$);

    // Handling client error event
    const clientErrorProm = utils.promise<never>();
    void clientErrorProm.p.catch(() => {}); // Ignore unhandled rejection
    client.addEventListener(
      events.EventQUICClientError.name,
      (evt: events.EventQUICClientError) => clientErrorProm.rejectP(evt.detail),
      { once: true },
    );

    // Handling client destroy event
    const clientDestroyedProm = utils.promise<void>();
    void clientDestroyedProm.p.catch(() => {}); // Ignore unhandled rejection
    client.addEventListener(
      events.EventQUICClientDestroyed.name,
      () => clientDestroyedProm.resolveP(),
      { once: true },
    );

    // @ts-ignore: kidnap protected property
    const clientSocket = client.socket;
    await clientSocket.stop({ force: true });

    // Socket failure triggers client connection local failure
    expect(await clientConnectionErrorP).toBeInstanceOf(
      errors.ErrorQUICConnectionLocal,
    );
    await expect(clientErrorProm.p).rejects.toThrow(
      errors.ErrorQUICClientSocketNotRunning,
    );
    await clientDestroyedProm.p;
    await clientConnectionClosedP;

    // Socket failure will not trigger any close frame since transport has failed so server connection will time out
    await firstValueFrom(serverConnection.timedOut$);

    await client.destroy({ force: true });
    await server.stop({ force: true });
  });

  test('connections share the same id information', async () => {
    const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);
    const server = new QUICServer({
      crypto: {
        key,
        ops: serverCryptoOps,
      },
      logger: logger.getChild(QUICServer.name),
      config: {
        key: tlsConfigServer.leafKeyPairPEM.privateKey,
        cert: tlsConfigServer.leafCertPEM,
        verifyPeer: false,
      },
    });
    socketCleanMethods.extractSocket(server);
    const serverConnectionP = firstValueFrom(server.connection$);
    await server.start({
      host: localhost,
    });
    // If the server is slow to respond then this will time out.
    //  Then main cause of this was the server not processing the initial packet
    //  that creates the `QUICConnection`, as a result, the whole creation waited
    //  an extra 1 second for the client to retry the initial packet.
    const client = await QUICClient.createQUICClient(
      {
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: false,
        },
      },
      timer(500),
    );
    socketCleanMethods.extractSocket(client);

    const clientConn = client.connection;
    const serverConn = await serverConnectionP;
    expect(clientConn.connectionId).toEqual(serverConn.connectionIdPeer);
    expect(clientConn.connectionIdPeer).toEqual(serverConn.connectionId);
    expect(clientConn.connectionIdShared).toEqual(
      serverConn.connectionIdShared,
    );

    await client.destroy({ force: true });
    await server.stop({ force: true });
  });

  test('handles many connections', async () => {
    const connNum = 100;
    const tlsConfigServer = await testsUtils.generateTLSConfig(defaultType);
    const connectionEventProm = utils.promise<void>();
    const server = new QUICServer({
      crypto: {
        key,
        ops: serverCryptoOps,
      },
      logger: logger.getChild(QUICServer.name),
      config: {
        key: tlsConfigServer.leafKeyPairPEM.privateKey,
        cert: tlsConfigServer.leafCertPEM,
        verifyPeer: false,
      },
    });
    socketCleanMethods.extractSocket(server);
    let connCount = 0;
    server.connection$.subscribe(() => {
      connCount++;
      if (connCount === connNum) connectionEventProm.resolveP();
    });
    await server.start({
      host: localhost,
    });
    const sharedSocket = new QUICSocket({
      logger: logger.getChild(QUICSocket.name),
    });
    await sharedSocket.start({
      host: localhost,
    });
    const clientPs: Array<Promise<QUICClient>> = [];
    for (let i = 0; i < connNum; i++) {
      const clientP = QUICClient.createQUICClient(
        {
          socket: sharedSocket,
          host: localhost,
          port: server.port,
          crypto: {
            ops: clientCryptoOps,
          },
          logger: logger.getChild(QUICClient.name),
          config: {
            verifyPeer: false,
          },
        },
        timer(2000),
      );
      clientPs.push(clientP);
    }
    const clients = await Promise.all(clientPs);
    await connectionEventProm.p;
    await Promise.all(clients.map((client) => client.destroy({ force: true })));
    await sharedSocket.stop({ force: true });
    await server.stop({ force: true });
  });

  describe.each(types)('TLS failures with %s certs', (type) => {
    test('client rejects servers TLS certificates due to wrong CA', async () => {
      const tlsConfig1 = await testsUtils.generateTLSConfig(type);
      const tlsConfig2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig1.leafKeyPairPEM.privateKey,
          cert: tlsConfig1.leafCertPEM,
        },
      });
      socketCleanMethods.extractSocket(server);
      await server.start({
        host: localhost,
      });
      const serverConnectionP = firstValueFrom(server.connection$);
      const client1p = QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          verifyPeer: true,
          ca: tlsConfig2.caCertPEM,
        },
      });
      const serverConnection = await serverConnectionP;
      expect(await firstValueFrom(serverConnection.error$)).toBeInstanceOf(
        errors.ErrorQUICConnectionPeerTLS,
      );
      await expect(client1p).rejects.toThrow(
        errors.ErrorQUICConnectionLocalTLS,
      );

      await server.stop();
    });
    test('server rejects clients TLS certificates due to wrong CA', async () => {
      const tlsConfig1 = await testsUtils.generateTLSConfig(type);
      const tlsConfig2 = await testsUtils.generateTLSConfig(type);
      const server = new QUICServer({
        crypto: {
          key,
          ops: serverCryptoOps,
        },
        logger: logger.getChild(QUICServer.name),
        config: {
          key: tlsConfig1.leafKeyPairPEM.privateKey,
          cert: tlsConfig1.leafCertPEM,
          verifyPeer: true,
        },
      });
      socketCleanMethods.extractSocket(server);
      const serverConnectionP = firstValueFrom(server.connection$);
      await server.start({
        host: localhost,
      });
      // When the server rejects the client, it happens after establishment.
      // So the client will still connect but then error due to the TLS.
      const client1 = await QUICClient.createQUICClient({
        host: localhost,
        port: server.port,
        localHost: localhost,
        crypto: {
          ops: clientCryptoOps,
        },
        logger: logger.getChild(QUICClient.name),
        config: {
          key: tlsConfig2.leafKeyPairPEM.privateKey,
          cert: tlsConfig2.leafCertPEM,
          verifyPeer: false,
        },
      });
      socketCleanMethods.extractSocket(client1);
      const serverConnection = await serverConnectionP;
      expect(await firstValueFrom(serverConnection.error$)).toBeInstanceOf(
        errors.ErrorQUICConnectionLocalTLS,
      );
      expect(await firstValueFrom(client1.connection.error$)).toBeInstanceOf(
        errors.ErrorQUICConnectionPeerTLS,
      );
      await firstValueFrom(client1.connection.closed$);
      await firstValueFrom(serverConnection.closed$);

      await client1.destroy({ force: true });
      await server.stop({ force: true });
    });
  });
});
