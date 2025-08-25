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
import { CryptoError, quiche, Shutdown } from './native/index.js';
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

export enum Step {
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

// TODO: A timedOut should count as an error condition and emit on error$.
// TODO: We havea timeout$ and timedout$, this is really confusing.

class QUICConnection {
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

  public readonly error$: Subject<Error> = new Subject();
  // A send event must be emitted after the following
  //  - when a recv is processed
  //  - After a timeout event when `onTimeout()` is called
  //  - When the application interacts with the streams
  public readonly send$: Subject<SendData> = new Subject();
  public readonly recvHandled$: Subject<void> = new Subject();
  public readonly step$: Subject<Step> = new Subject();

  /**
   * Chain of local certificates from leaf to root in DER format.
   */
  protected certDERs: Array<Uint8Array> = [];

  /**
   * Array of independent CA certificates in DER format.
   */
  protected caDERs: Array<Uint8Array> = [];

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

  // Sets everything up
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

  public get connectionId(): ConnectionIdString {
    const sourceId = this.connection.sourceId();
    return Buffer.from(sourceId).toString('hex') as ConnectionIdString;
  }

  public get connectionIdPeer(): ConnectionIdString {
    const destinationId = this.connection.destinationId();
    return Buffer.from(destinationId).toString('hex') as ConnectionIdString;
  }

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

  public get closed() {
    return this.connection.isClosed();
  }

  /**
   * This just shoves data into the underlying connection instance and triggers observable events
   */
  public processRecv(data: Uint8Array, remoteInfo: RemoteInfo) {
    this.step$.next(Step.RecvBegin);
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
      this.logger.warn('failed here');
      if (e.message === 'TlsFail') {
        // TODO: error out TLS observable
        const error = this.localError;
        if (error == null) utils.never('local error information as missing');
        const message = `TLS verification failed with ${
          CryptoError[error.errorCode]
        }(${error.errorCode})`;
        // TODO: make a proper TLS fail error
        this.error$.next(Error(`TMP IMP ` + message));
      } else {
        this.error$.next(Error(`TMP IMP Errored from ${e.message}`));
      }
    }
    this.recvHandled$.next();
    this.processStreams();
    this.processSend();
    this.step$.next(Step.RecvEnd);
  }

  // This will extract send data from the connection and emit it on the `sendData$` subject
  public processSend(): void {
    this.step$.next(Step.SendBegin);
    try {
      if (this.connection.isDraining()) return;
      while (true) {
        const sendBuffer = Buffer.allocUnsafe(
          this.config.maxSendUdpPayloadSize,
        );
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
    } catch (e) {
      // TODO: dispatch connection error
      this.logger.warn(`failed to process send with ${e.message}`);
      throw e;
    } finally {
      this.checkState();
    }
    this.step$.next(Step.SendEnd);
  }

  protected checkState(): void {
    this.step$.next(Step.StateBegin);
    this.checkTimeout();
    void this.isEstablished;
    void this.isResumed;
    void this.isInEarlyData;
    void this.isReadable;
    void this.isDraining;
    void this.isTimedOut;
    void this.peerError;
    void this.localError;
    void this.peerCertChain;
    void this.isClosed;
    this.step$.next(Step.StateEnd);
  }

  protected timeoutTimer: NodeJS.Timeout | undefined;
  public readonly timeout$: Subject<void> = new Subject();
  protected handleTimeout = () => {
    this.connection.onTimeout();
    this.timeout$.next();
    this.processSend();
    this.checkState();
    this.checkTimeout();
  };
  protected checkTimeout() {
    const timeoutDelay = this.connection.timeout();
    clearTimeout(this.timeoutTimer);
    delete this.timeoutTimer;
    if (timeoutDelay == null) {
      return;
    }
    this.timeoutTimer = setTimeout(this.handleTimeout, timeoutDelay + 1);
  }

  protected isEstablished_ = false;
  public readonly established$: ReplaySubject<void> = new ReplaySubject(1);
  public get isEstablished() {
    const value = this.connection.isEstablished();
    const updated = value !== this.isEstablished_;
    this.isEstablished_ = value;
    if (updated) this.established$.next();
    return value;
  }

  protected isResumed_ = false;
  public readonly isResumed$: Subject<boolean> = new Subject();
  public get isResumed() {
    const value = this.connection.isResumed();
    const updated = value !== this.isResumed_;
    this.isResumed_ = value;
    if (updated) this.isResumed$.next(value);
    return value;
  }

  protected isInEarlyData_ = false;
  public readonly isInEarlyData$: Subject<boolean> = new Subject();
  public get isInEarlyData() {
    const value = this.connection.isInEarlyData();
    const updated = value !== this.isInEarlyData_;
    this.isInEarlyData_ = value;
    if (updated) this.isInEarlyData$.next(value);
    return value;
  }

  protected isReadable_ = false;
  public readonly isReadable$: Subject<boolean> = new Subject();
  public get isReadable() {
    const value = this.connection.isReadable();
    const updated = value !== this.isReadable_;
    this.isReadable_ = value;
    if (updated) this.isReadable$.next(value);
    return value;
  }

  protected isDraining_ = false;
  public readonly draining$: ReplaySubject<void> = new ReplaySubject(1);
  public get isDraining() {
    const value = this.connection.isDraining();
    const updated = value !== this.isDraining_;
    this.isDraining_ = value;
    if (updated) this.draining$.next();
    return value;
  }

  protected isClosed_ = false;
  public readonly closed$: ReplaySubject<void> = new ReplaySubject(1);
  public get isClosed() {
    const value = this.connection.isClosed();
    const updated = value !== this.isClosed_;
    this.isClosed_ = value;
    if (updated) this.closed$.next();
    return value;
  }

  protected isTimedOut_ = false;
  public readonly timedOut$: ReplaySubject<void> = new ReplaySubject(1);
  public get isTimedOut() {
    const value = this.connection.isTimedOut();
    const updated = value !== this.isTimedOut_;
    this.isTimedOut_ = value;
    if (updated) this.timedOut$.next();
    return value;
  }

  protected peerError_: nativeTypes.ConnectionError | undefined = undefined;
  public readonly peerError$: ReplaySubject<nativeTypes.ConnectionError> =
    new ReplaySubject(1);
  public get peerError() {
    const value = this.connection.peerError();
    if (this.peerError_ == null && value != null) {
      this.peerError_ = value;
      this.peerError$.next(value);
      this.error$.next(
        new errors.ErrorQUICConnectionPeer(undefined, { data: value }),
      );
    }
    return value;
  }

  protected localError_: nativeTypes.ConnectionError | undefined = undefined;
  public readonly localError$: ReplaySubject<nativeTypes.ConnectionError> =
    new ReplaySubject(1);
  public get localError() {
    const value = this.connection.localError();
    if (this.localError_ == null && value != null) {
      this.localError_ = value;
      this.localError$.next(value);
      this.error$.next(
        new errors.ErrorQUICConnectionLocal(undefined, { data: value }),
      );
    }
    return value;
  }

  protected peerCertChain_: Array<Uint8Array> | undefined = undefined;
  public readonly peerCertChain$: ReplaySubject<Array<Uint8Array>> =
    new ReplaySubject(1);
  // Get peer cert chain in DER format
  public get peerCertChain(): Array<Uint8Array> | undefined {
    const value = this.connection.peerCertChain();
    if (this.peerCertChain_ == null && value != null) {
      this.peerCertChain_ = value;
      this.peerCertChain$.next(this.peerCertChain_);
    }
    return this.peerCertChain_;
  }

  public get openStreams(): number {
    return this.streamMap.size;
  }

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

  // Stream related methods

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

  // Process streams by iterating over the readable and writable iterators
  // and letting the streams know there is data.
  public processStreams(): void {
    this.step$.next(Step.StreamsBegin);
    this.step$.next(Step.StreamsWritableBegin);
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
      this.step$.next(Step.StreamCreateWritable);
    }
    this.step$.next(Step.StreamsWritableEnd);
    this.step$.next(Step.StreamsReadableBegin);
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
        this.step$.next(Step.StreamCreateReadable);
      }
      quicStream.readReady$.next();
    }
    this.step$.next(Step.StreamsReadableEnd);
    this.step$.next(Step.StreamsEnd);
  }

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

  protected setupQuicStream(quicStream: QUICStream): void {
    this.streamMap.set(quicStream.id, quicStream);
    quicStream.complete$.subscribe(() => {
      this.streamMap.delete(quicStream.id);
    });
  }
}

export default QUICConnection;
