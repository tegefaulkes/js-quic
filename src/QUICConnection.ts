import type { Connection, RecvInfo } from './native/index.js';
import type * as nativeTypes from './native/types.js';
import type Logger from '@matrixai/logger';
import type {
  ConnectionIdString,
  Host,
  Port,
  QUICConfig,
  RemoteInfo,
  SendData,
  StreamCodeToReason,
  StreamId,
  StreamReasonToCode,
} from './types.js';
import { firstValueFrom, ReplaySubject, Subject } from 'rxjs';
import { quiche, Shutdown } from './native/index.js';
import {
  ConnectionType,
  CLIENT_BIDI_ID,
  SERVER_BIDI_ID,
  CLIENT_UNI_ID,
  SERVER_UNI_ID,
} from './types.js';
import { buildQuicheConfig } from './config.js';
import * as errors from './errors.js';
import * as utils from './utils.js';
import QUICConnectionId from './QUICConnectionId.js';
import QUICStream from './QUICStream.js';

/**
 * ENUM for mapping connection processing steps to simple codes.
 * This is used to avoid too much processing during low level connection events.
 */
export enum ConnectionStep {
  RecvBegin = 0,
  RecvEnd = 1,
  StreamsBegin = 2,
  StreamsEnd = 3,
  StreamsWritableBegin = 4,
  StreamsWritableEnd = 5,
  StreamsReadableBegin = 6,
  StreamsReadableEnd = 7,
  SendBegin = 8,
  SendEnd = 9,
  StreamCreateWritable = 10,
  StreamCreateReadable = 11,
  StateBegin = 12,
  StateEnd = 13,
}

/**
 * This wraps the underlying quiche connection state and provides an event based interface.
 */
class QUICConnection {
  /**
   * Creates a client initiated QUICConnection.
   * @param serverName - client connections can use server name for
   *                          verifying the server's certificate. however, if
   *                          `config.verifyCallback` is set, this will have no
   *                          effect.
   * @param scid - source connection ID.
   * @param config - QUIC config.
   * @param sourceHost - source host for the connection.
   * @param sourcePort - Source port for the connection.
   * @param host - Target host for the connection.
   * @param port - Target port for the connection.
   * @param codeToReason - maps stream error reasons to stream error codes.
   * @param reasonToCode - maps stream error codes to reasons.
   * @param logger
   */
  static connectionConnect({
    serverName,
    scid,
    config,
    sourceHost,
    sourcePort,
    host,
    port,
    codeToReason = (type, code) =>
      new Error(`${type.toString()} ${code.toString()}`),
    reasonToCode = () => 1,
    logger,
  }: {
    serverName?: string;
    config: QUICConfig;
    scid: Uint8Array;
    sourceHost: Host;
    sourcePort: Port;
    host: Host;
    port: Port;
    codeToReason?: StreamCodeToReason;
    reasonToCode?: StreamCodeToReason;
    logger: Logger;
  }): QUICConnection {
    // Doing checks.
    if (
      config.keepAliveIntervalTime != null &&
      config.maxIdleTimeout !== 0 &&
      config.keepAliveIntervalTime >= config.maxIdleTimeout
    ) {
      throw new errors.ErrorQUICConnectionConfigInvalid(
        '`keepAliveIntervalTime` must be less than `maxIdleTimeout`',
      );
    }
    if (scid.byteLength !== quiche.MAX_CONN_ID_LEN) {
      throw Error(
        `connection id byte-length must be ${quiche.MAX_CONN_ID_LEN}`,
      );
    }

    const quicheConfig = buildQuicheConfig(config);

    const connection = quiche.Connection.connect(
      serverName,
      scid,
      {
        host: sourceHost,
        port: sourcePort,
      },
      {
        host: host,
        port: port,
      },
      quicheConfig,
    );
    // This will output to the log keys file path
    if (config.logKeys != null) {
      connection!.setKeylog(config.logKeys);
    }
    return new this(
      ConnectionType.CLIENT,
      connection,
      config,
      sourceHost,
      sourcePort,
      host,
      port,
      codeToReason,
      reasonToCode,
      logger,
    );
  }

  /**
   * Creates a server received QUICConnection.
   * @param scid - source connection ID.
   * @param dcid - destination connection ID.
   * @param config - QUIC config.
   * @param sourceHost - source host for the connection.
   * @param sourcePort - Source port for the connection.
   * @param host - Target host for the connection.
   * @param port - Target port for the connection.
   * @param codeToReason - maps stream error reasons to stream error codes.
   * @param reasonToCode - maps stream error codes to reasons.
   * @param logger
   */
  static connectionAccept({
    scid,
    dcid,
    config,
    sourceHost,
    sourcePort,
    host,
    port,
    codeToReason = (type, code) =>
      new Error(`${type.toString()} ${code.toString()}`),
    reasonToCode = () => 1,
    logger,
  }: {
    serverName?: string;
    config: QUICConfig;
    scid: QUICConnectionId;
    dcid: QUICConnectionId;
    sourceHost: Host;
    sourcePort: Port;
    host: Host;
    port: Port;
    codeToReason?: StreamCodeToReason;
    reasonToCode?: StreamCodeToReason;
    logger: Logger;
  }) {
    // Doing checks.
    if (
      config.keepAliveIntervalTime != null &&
      config.maxIdleTimeout !== 0 &&
      config.keepAliveIntervalTime >= config.maxIdleTimeout
    ) {
      throw new errors.ErrorQUICConnectionConfigInvalid(
        '`keepAliveIntervalTime` must be less than `maxIdleTimeout`',
      );
    }
    const quicheConfig = buildQuicheConfig(config);
    const connection = quiche.Connection.accept(
      scid,
      dcid,
      {
        host: sourceHost,
        port: sourcePort,
      },
      {
        host: host,
        port: port,
      },
      quicheConfig,
    );
    // This will output to the log keys file path
    if (config.logKeys != null) {
      connection!.setKeylog(config.logKeys);
    }
    return new this(
      ConnectionType.SERVER,
      connection,
      config,
      sourceHost,
      sourcePort,
      host,
      port,
      codeToReason,
      reasonToCode,
      logger,
    );
  }

  /**
   * Emitts an Error when an Error has occurred.
   */
  public readonly error$: ReplaySubject<Error> = new ReplaySubject(1);
  // A send event must be emitted after the following
  //  - when a recv is processed
  //  - After a timeout event when `onTimeout()` is called
  //  - When the application interacts with the streams
  /**
   * Emits data that needs to be sent.
   */
  public readonly send$: Subject<SendData> = new Subject();
  /**
   * Emits when a recv is processed. Currently just used to process stream finish in certain conditions.
   */
  public readonly recvHandled$: Subject<void> = new Subject();
  /**
   * General debug observable that emits ConnectionStep enum as events.
   */
  public readonly connectionEvents$: Subject<ConnectionStep> = new Subject();

  /**
   * Chain of local certificates from leaf to root in DER format.
   */
  protected certDERs: Array<Uint8Array> = [];

  /**
   * Array of independent CA certificates in DER format.
   */
  protected caDERs: Array<Uint8Array> = [];

  /**
   * True when the QUICConnection is rejecting new connections.
   */
  protected rejectStreams: boolean = false;

  /**
   * Keep alive timer.
   *
   * Quiche does not natively ensure activity on the connection. This interval
   * timer guarantees that there will be activity on the connection by sending
   * acknowledgement eliciting frames, which will cause the peer to acknowledge.
   *
   * This is still useful even if the `config.maxIdleTimeout` is set to 0, which
   * means the connection will never time out due to being idle.
   *
   * This mechanism will only start working after `secureEstablishedP`.
   */
  protected keepAliveIntervalTimer?: NodeJS.Timeout;

  public constructor(
    public readonly type: ConnectionType,
    public readonly connection: Connection,
    public readonly config: QUICConfig,
    public readonly sourceHost: Host,
    public readonly sourcePort: Port,
    public readonly host: Host,
    public readonly port: Port,
    protected codeToReason: StreamCodeToReason,
    protected reasonToCode: StreamReasonToCode,
    protected logger: Logger,
  ) {
    if (config.cert != null) {
      const certPEMs = utils.collectPEMs(this.config.cert);
      this.certDERs = certPEMs.map(utils.pemToDER);
    }
    if (this.config.ca != null) {
      const caPEMs = utils.collectPEMs(this.config.ca);
      this.caDERs = caPEMs.map(utils.pemToDER);
    }
    if (this.config.keepAliveIntervalTime != null) {
      const establishedSubscription = this.established$.subscribe(() => {
        this.keepAliveIntervalTimer = setInterval(() => {
          this.connection.sendAckEliciting();
          this.processSend();
        }, this.config.keepAliveIntervalTime);
        establishedSubscription.unsubscribe();
      });
    }
    // Trigger ending stream if the connection closes
    this.closed$.subscribe(() => {
      clearTimeout(this.keepAliveIntervalTimer);
      if (this.isTimedOut_) {
        void this.endStreams(true, new errors.ErrorQUICConnectionIdleTimeout());
        return;
      }
      void this.endStreams(true, new errors.ErrorQUICConnectionClosed());
      return;
    });
  }

  public get connectionId_(): QUICConnectionId {
    const sourceId = this.connection.sourceId();
    // Zero copy construction of QUICConnectionId
    return new QUICConnectionId(
      sourceId.buffer,
      sourceId.byteOffset,
      sourceId.byteLength,
    );
  }

  /**
   * Local connection ID as a hex string.
   */
  public get connectionId(): ConnectionIdString {
    const sourceId = this.connection.sourceId();
    return Buffer.from(sourceId).toString('hex') as ConnectionIdString;
  }

  /**
   * Peer connection ID as a hex string.
   */
  public get connectionIdPeer(): ConnectionIdString {
    const destinationId = this.connection.destinationId();
    return Buffer.from(destinationId).toString('hex') as ConnectionIdString;
  }

  /**
   * Connection ID that is shared between the client and server as a hex string.
   */
  public get connectionIdShared(): string {
    const sourceId = this.connectionId;
    const destinationId = this.connectionIdPeer;
    return [sourceId, destinationId].sort().join('-');
  }

  /**
   * Array of independent CA certificates in DER format.
   */
  public get getLocalCACertsChain(): Array<Uint8Array> {
    return this.caDERs;
  }

  /**
   * Chain of local certificates from leaf to root in DER format.
   */
  public get getLocalCertsChain(): Array<Uint8Array> {
    return this.certDERs;
  }

  /**
   * Processes incoming packets.
   * Once the packet has been processed, the streams are then processed and any outgoing packets are sent.
   */
  public processRecv(data: Uint8Array, remoteInfo: RemoteInfo) {
    this.connectionEvents$.next(ConnectionStep.RecvBegin);
    const recvInfo: RecvInfo = {
      to: {
        host: this.sourceHost,
        port: this.sourcePort,
      },
      from: {
        host: remoteInfo.host,
        port: remoteInfo.port,
      },
    };
    try {
      this.connection.recv(data, recvInfo);
    } catch (e) {
      this.logger.warn(`failed here with ${e.message}`);
      // Catch and ignore the error. Error handling will be done when the state is checked later
    }
    this.recvHandled$.next();
    this.processStreams();
    this.processSend();
    this.connectionEvents$.next(ConnectionStep.RecvEnd);
  }

  /**
   * This will extract send data from the connection and emit it on the `sendData$` subject.
   * Will check the connection state after data has been sent.
   */
  public processSend(): void {
    this.connectionEvents$.next(ConnectionStep.SendBegin);
    try {
      if (this.connection.isDraining()) return;
      while (true) {
        const sendBuffer = Buffer.allocUnsafe(
          this.config.maxSendUdpPayloadSize,
        );
        // This could error out under some conditions. For now, we're treating it as a fatal error until it becomes a problem
        const result = this.connection.send(sendBuffer);
        if (result == null) break;
        const [sendLength, sendInfo] = result;
        this.send$.next({
          data: sendBuffer.subarray(0, sendLength),
          host: sendInfo.to.host,
          port: sendInfo.to.port,
          at: sendInfo.at,
        });
      }
    } finally {
      this.checkState();
    }
    this.connectionEvents$.next(ConnectionStep.SendEnd);
  }

  /**
   * Checks the state of the connection and emits events if necessary.
   */
  protected checkState(): void {
    this.connectionEvents$.next(ConnectionStep.StateBegin);
    this.CheckTickTimer();
    void this.isEstablished;
    void this.isInEarlyData;
    void this.isReadable;
    void this.isDraining;
    void this.isTimedOut;
    void this.peerError;
    void this.localError;
    void this.peerCertChain;
    void this.isClosed;
    this.connectionEvents$.next(ConnectionStep.StateEnd);
  }

  /**
   * Timer for the tick timeout
   */
  protected tickTimer: NodeJS.Timeout | undefined;
  /**
   * A tick event is emitted when a time-based state change occurs.
   */
  public readonly tick$: Subject<void> = new Subject();
  protected handleTick = () => {
    this.connection.onTimeout();
    this.tick$.next();
    this.processSend();
    this.checkState();
    this.CheckTickTimer();
  };
  protected CheckTickTimer() {
    const timeoutDelay = this.connection.timeout();
    clearTimeout(this.tickTimer);
    delete this.tickTimer;
    if (timeoutDelay == null) {
      return;
    }
    this.tickTimer = setTimeout(this.handleTick, timeoutDelay + 1);
  }

  protected isEstablished_ = false;
  /**
   * Emits once the connection is established. Establishment is separate from TLS handshake completion.
   */
  public readonly established$: ReplaySubject<void> = new ReplaySubject(1);
  /**
   * Returns true once the connection is established. Establishment is separate from TLS handshake completion.
   */
  public get isEstablished() {
    const value = this.connection.isEstablished();
    const updated = value !== this.isEstablished_;
    this.isEstablished_ = value;
    if (updated) this.established$.next();
    return value;
  }

  protected isInEarlyData_ = false;
  /**
   * Emits once the connection has reached the early data stage.
   */
  public readonly isInEarlyData$: ReplaySubject<boolean> = new ReplaySubject(1);
  /**
   * Returns true once the connection has reached the early data stage.
   */
  public get isInEarlyData() {
    const value = this.connection.isInEarlyData();
    const updated = value !== this.isInEarlyData_;
    this.isInEarlyData_ = value;
    if (updated) this.isInEarlyData$.next(value);
    return value;
  }

  public get isReadable() {
    return this.connection.isReadable();
  }

  protected isDraining_ = false;
  /**
   * Emits once the connection has reached the draining stage.
   * Draining means the connection is awaiting acknowledgement of closing and will not be sending any new packets.
   * The connection will close after an acknowledgement is received, or a timeout occurs.
   */
  public readonly draining$: ReplaySubject<void> = new ReplaySubject(1);
  /**
   * Returns true once the connection has reached the draining stage.
   * Draining means the connection is awaiting acknowledgement of closing and will not be sending any new packets.
   * The connection will close after an acknowledgement is received, or a timeout occurs.
   */
  public get isDraining() {
    const value = this.connection.isDraining();
    const updated = value !== this.isDraining_;
    this.isDraining_ = value;
    if (updated) this.draining$.next();
    return value;
  }

  protected isClosed_ = false;
  /**
   * Emits once the connection has closed.
   * No more events will happen after this stage.
   */
  public readonly closed$: ReplaySubject<void> = new ReplaySubject(1);
  /**
   * Returns true once the connection has closed.
   * No more events will happen after this stage.
   */
  public get isClosed() {
    const value = this.connection.isClosed();
    const updated = value !== this.isClosed_;
    this.isClosed_ = value;
    if (updated) this.closed$.next();
    return value;
  }

  protected isTimedOut_ = false;
  /**
   *  Emits if the connection closed due to timing out.
   */
  public readonly timedOut$: ReplaySubject<void> = new ReplaySubject(1);
  /**
   *  Returns true if the connection closed due to timing out.
   */
  public get isTimedOut() {
    const value = this.connection.isTimedOut();
    const updated = value !== this.isTimedOut_;
    this.isTimedOut_ = value;
    if (updated) this.timedOut$.next();
    // This.error$.next(new errors.ErrorQUICConnectionIdleTimeout());
    return value;
  }

  protected peerError_: nativeTypes.ConnectionError | undefined = undefined;
  /**
   * Emits when a peer error occurs.
   */
  public readonly peerError$: ReplaySubject<nativeTypes.ConnectionError> =
    new ReplaySubject(1);
  /**
   * Returns the peer error if the connection errored due to the peer.
   */
  public get peerError() {
    const value = this.connection.peerError();
    if (this.peerError_ == null && value != null) {
      this.peerError_ = value;
      this.peerError$.next(value);
      if (utils.isCryptoErrorCode(value.errorCode)) {
        this.error$.next(
          new errors.ErrorQUICConnectionPeerTLS(undefined, { data: value }),
        );
      } else {
        this.error$.next(
          new errors.ErrorQUICConnectionPeer(undefined, { data: value }),
        );
      }
    }
    return value;
  }

  protected localError_: nativeTypes.ConnectionError | undefined = undefined;
  /**
   * Emits when a peer error occurs.
   */
  public readonly localError$: ReplaySubject<nativeTypes.ConnectionError> =
    new ReplaySubject(1);
  /**
   * Returns the local error if the connection errored due to the local connection.
   */
  public get localError() {
    const value = this.connection.localError();
    if (this.localError_ == null && value != null) {
      this.localError_ = value;
      this.localError$.next(value);
      if (utils.isCryptoErrorCode(value.errorCode)) {
        this.error$.next(
          new errors.ErrorQUICConnectionLocalTLS(undefined, { data: value }),
        );
      } else {
        this.error$.next(
          new errors.ErrorQUICConnectionLocal(undefined, { data: value }),
        );
      }
    }
    return value;
  }

  protected peerCertChain_: Array<Uint8Array> | undefined = undefined;
  /**
   * Emits the peer certificate chain in DER format once the peer has been verified.
   */
  public readonly peerCertChain$: ReplaySubject<Array<Uint8Array>> =
    new ReplaySubject(1);
  /**
   * Returns the peer certificate chain in DER format once the peer has been verified.
   */
  public get peerCertChain(): Array<Uint8Array> | undefined {
    const value = this.connection.peerCertChain();
    if (this.peerCertChain_ == null && value != null) {
      this.peerCertChain_ = value;
      this.peerCertChain$.next(this.peerCertChain_);
    }
    return this.peerCertChain_;
  }

  /**
   * Returns the number of streams that are currently open.
   */
  public get openStreams(): number {
    return this.streamMap.size;
  }

  /**
   * Signals the connection to reject new streams and wait for all existing streams to complete.
   * If forced, it will trigger the streams to kill, otherwise it will just wait for their completion.
   * @param force - Trigger all existing streams to kill themselves.
   * @param reason - If provided the streams will be killed with this reason.
   */
  public async endStreams(force: boolean, reason?: Error): Promise<void> {
    // Prevent new streams from being created
    this.rejectStreams = true;
    // Wait for all streams to end
    const streams: Array<Promise<void>> = [];
    for (const [, stream] of this.streamMap) {
      streams.push(firstValueFrom(stream.complete$));
      // If forced, then we need to trigger the stream to end
      if (force) stream.kill(reason);
    }
    await Promise.all(streams);
  }

  /**
   * Used to kill the current connection. This will not trigger the existing streams to end until after the connection is closed.
   * @param isApp - Whether the application initiated the connection closure.
   * @param errorCode - The error code to use for the connection closure.
   * @param reason - The reason message provided for the connection closure.
   */
  public kill({
    isApp,
    errorCode,
    reason,
  }: {
    isApp: boolean;
    errorCode: number;
    reason: Uint8Array;
  }) {
    this.connection.close(isApp, errorCode, reason);
    this.processSend();
  }

  /**
   * Client-initiated bidirectional stream starts at 0.
   * Increment by 4 to get the next ID.
   */
  protected streamIdClientBidi: StreamId = CLIENT_BIDI_ID;

  /**
   * Server initiated bidirectional stream starts at 1.
   * Increment by 4 to get the next ID.
   */
  protected streamIdServerBidi: StreamId = SERVER_BIDI_ID;

  /**
   * Client initiated unidirectional stream starts at 2.
   * Increment by 4 to get the next ID.
   */
  protected streamIdClientUni: StreamId = CLIENT_UNI_ID;

  /**
   * Server initiated unidirectional stream starts at 3.
   * Increment by 4 to get the next ID.
   */
  protected streamIdServerUni: StreamId = SERVER_UNI_ID;

  /**
   * The step each new stream ID is incremented
   */
  protected streamIdStep: StreamId = 4;

  protected streamMap: Map<StreamId, QUICStream> = new Map();

  public stream$: Subject<QUICStream> = new Subject();

  /**
   * Process streams by iterating over the readable and writable iterators and letting the streams know there is data.
   * It will call each stream's `writeReady$` and `readReady$` subjects to signal if it is ready to read or write.
   */
  public processStreams(): void {
    this.connectionEvents$.next(ConnectionStep.StreamsBegin);
    this.connectionEvents$.next(ConnectionStep.StreamsWritableBegin);
    for (const streamId of this.connection.writable()) {
      let quicStream = this.streamMap.get(streamId);
      if (quicStream == null) {
        if (this.rejectStreams) {
          this.rejectStream(streamId);
          continue;
        }

        quicStream = new QUICStream(
          streamId,
          this,
          this.codeToReason,
          this.reasonToCode,
        );
        this.setupQuicStream(quicStream);
        this.stream$.next(quicStream);
      }
      quicStream.writeReady$.next();
      this.connectionEvents$.next(ConnectionStep.StreamCreateWritable);
    }
    this.connectionEvents$.next(ConnectionStep.StreamsWritableEnd);
    this.connectionEvents$.next(ConnectionStep.StreamsReadableBegin);
    for (const streamId of this.connection.readable()) {
      let quicStream = this.streamMap.get(streamId);
      if (quicStream == null) {
        if (this.rejectStreams) {
          this.rejectStream(streamId);
          continue;
        }
        quicStream = new QUICStream(
          streamId,
          this,
          this.codeToReason,
          this.reasonToCode,
        );
        this.setupQuicStream(quicStream);
        this.stream$.next(quicStream);
        this.connectionEvents$.next(ConnectionStep.StreamCreateReadable);
      }
      quicStream.readReady$.next();
    }
    this.connectionEvents$.next(ConnectionStep.StreamsReadableEnd);
    this.connectionEvents$.next(ConnectionStep.StreamsEnd);
  }

  /**
   * An internal method to handle rejecting streams.
   */
  protected rejectStream(streamId: StreamId): void {
    const error = new errors.ErrorQUICConnectionDraining();
    this.connection.streamShutdown(
      streamId,
      Shutdown.Read,
      this.reasonToCode('read', error),
    );
    this.connection.streamShutdown(
      streamId,
      Shutdown.Write,
      this.reasonToCode('write', error),
    );
  }

  /**
   * Creates a new QUIC stream on the connection.
   */
  public newStream(type: 'bidi' | 'uni' = 'bidi'): QUICStream {
    if (this.rejectStreams) {
      throw new errors.ErrorQUICConnectionDraining();
    }
    let streamId: StreamId;
    if (this.type === ConnectionType.CLIENT && type === 'bidi') {
      streamId = this.streamIdClientBidi;
      this.streamIdClientBidi += this.streamIdStep;
    } else if (this.type === ConnectionType.SERVER && type === 'bidi') {
      streamId = this.streamIdServerBidi;
      this.streamIdServerBidi += this.streamIdStep;
    } else if (this.type === ConnectionType.CLIENT && type === 'uni') {
      streamId = this.streamIdClientUni;
      this.streamIdClientUni += this.streamIdStep;
    } else if (this.type === ConnectionType.SERVER && type === 'uni') {
      streamId = this.streamIdServerUni;
      this.streamIdServerUni += this.streamIdStep;
    }
    try {
      this.connection.streamSend(streamId!, Buffer.alloc(0), false);
    } catch (e) {
      if (e.message === 'StreamLimit') throw new errors.ErrorQUICStreamLimit();
      throw e;
    }
    const quicStream = new QUICStream(
      streamId!,
      this,
      this.codeToReason,
      this.reasonToCode,
    );
    this.setupQuicStream(quicStream);
    this.processSend();
    return quicStream;
  }

  /**
   * Internal method for adding new streams to the stream map and hooking up clean-up logic.
   */
  protected setupQuicStream(quicStream: QUICStream): void {
    this.streamMap.set(quicStream.id, quicStream);
    quicStream.complete$.subscribe(() => {
      this.streamMap.delete(quicStream.id);
    });
  }
}

export default QUICConnection;
