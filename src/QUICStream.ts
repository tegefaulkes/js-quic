import type QUICConnection from './QUICConnection.js';
import type {
  Host,
  Port,
  StreamCodeToReason,
  StreamId,
  StreamReasonToCode,
} from './types.js';
import { WritableStream, ReadableStream } from 'stream/web';
import { firstValueFrom, ReplaySubject, Subject } from 'rxjs';
import { CLIENT_BIDI_ID, SERVER_BIDI_ID } from './types.js';
import { CLIENT_UNI_ID, ConnectionType, SERVER_UNI_ID } from './types.js';
import * as utils from './utils.js';
import * as errors from './errors.js';
import { Shutdown } from './native/index.js';

/**
 * ENUM for mapping stream state events to simple codes.
 * This is used to avoid too much processing during low level stream events.
 */
export enum StreamState {
  WritableStart = 0,
  WritableWriteStart = 1,
  WritableWriteDone = 2,
  WritableErrored = 3,
  WritableBlocked = 4,
  WritableUnblocked = 5,
  WritableSentPartial = 6,
  WritableSentFull = 7,
  WritableClosed = 8,
  WritableAborted = 9,
  ReadableStart = 10,
  ReadablePullStart = 11,
  ReadableBlocked = 12,
  ReadableUnblocked = 13,
  ReadableRead = 14,
  ReadableFinished = 15,
  ReadableCancelled = 16,
  ReadableErrored = 17,
  Killed = 18,
}

/**
 * Chunk size allocated when reading data from the stream.
 * This is sized to contain a single packet worth of data.
 */
const CHUNK_SIZE = 1307;

/**
 * QUICStream is a wrapper around the QUICConnection stream methods for tracking stream state and processing stream data.
 * This functions as a ReadableWritableStreamPair with stream state, metadata and observable events included.
 *
 * Unidirection streams are initiated with either the readable or writable stream already errored out.
 * Bidirectional streams are initiated with both streams working.
 *
 * QUICStreams exist as pairs, with the forward QUICStream existing on the initiating side and the reverse stream existing
 * on the receiving side. This is separate from the client and server convention since all sides can initiate streams.
 *
 * The reverse stream is only created on the receiving side when data is sent. In the case that the forward writable
 * stream is closed before sending data, then the receiving readable stream is created as already complete. The same happens
 * when the stream is errored out before sending data.
 *
 * The QUICStream only completes once the readable and writable streams are complete. Completion can be a graceful
 * finish of the streams or an error condition causing it to close. The readable stream will only gracefully end once
 * all data has been read from it. Or in the case of it being already drained with a 0-len fin packet, it will close
 * immediately. An error on the readable stream will cause all remaining buffered data to be dropped.
 */
class QUICStream {
  /**
   * The writable side of the stream. If this is the initiated `QUICStream` then this is considered the forward side of the stream.
   */
  public readonly writable: WritableStream<Buffer>;
  /**
   * The readable side of the stream. If this is the initiated `QUICStream` then this is considered the reverse side of the stream.
   */
  public readonly readable: ReadableStream<Buffer>;
  /**
   * Used to signal that there is readable stream data available to be read.
   */
  public readonly readReady$: Subject<void> = new Subject();
  /**
   * Used to signal that the writable stream has capacity to write.
   */
  public readonly writeReady$: Subject<void> = new Subject();
  /**
   * General debug observable that emits StreamState enum as events.
   */
  public readonly streamEvents$: Subject<StreamState> = new Subject();

  protected writableAborted = false;
  protected readableCancelled = false;
  protected readableController: ReadableStreamController<Buffer>;
  protected writableController: WritableStreamDefaultController;

  protected writableStart: UnderlyingSinkStartCallback = (controller) => {
    this.streamEvents$.next(StreamState.WritableStart);
    this.writableController = controller;
  };
  protected writableWrite: UnderlyingSinkWriteCallback<Buffer> = async (
    chunk,
    controller,
  ) => {
    this.streamEvents$.next(StreamState.WritableWriteStart);
    // Check if capacity matches
    let remaining = chunk;
    while (true) {
      if (this.writableAborted) break;
      if (remaining.byteLength <= 0) break;
      let sentBytes: number | null = null;
      try {
        sentBytes = this.connection.connection.streamSend(
          this.id,
          remaining,
          false,
        );
      } catch (e) {
        this.streamEvents$.next(StreamState.WritableErrored);
        let error: Error;
        const code = utils.isStreamStopped(e);
        if (code != null) {
          error = this.codeToReason('write', code);
        } else {
          error = new errors.ErrorQUICStreamInternal(
            `stream send failed due to ${e.message}`,
            { cause: e },
          );
        }
        controller.error(error);
        this.updateWritableComplete();
        return;
      }
      if (sentBytes == null) {
        // No bytes sent, wait for more capacity
        this.streamEvents$.next(StreamState.WritableBlocked);
        await firstValueFrom(this.writeReady$);
        this.streamEvents$.next(StreamState.WritableUnblocked);
        continue;
      }
      if (sentBytes < remaining.byteLength) {
        // Partially sent, so we split the buffer
        remaining = remaining.subarray(sentBytes);
        this.streamEvents$.next(StreamState.WritableSentPartial);
        // Wait for more capacity
        this.connection.processSend();
        continue;
      }
      if (sentBytes >= remaining.byteLength) {
        this.streamEvents$.next(StreamState.WritableSentFull);
        this.connection.processSend();
        // Fully sent so break out and wait for more data
        break;
      }
      // Base case, if nothing happened, then we had some undefined logic
      utils.never('unexpected case happened and potential infinite loop');
    }
    this.streamEvents$.next(StreamState.WritableWriteDone);
  };
  protected writableClose: UnderlyingSinkCloseCallback = () => {
    this.streamEvents$.next(StreamState.WritableClosed);
    this.connection.connection.streamSend(this.id, Buffer.alloc(0), true);
    this.connection.processSend();
    this.updateWritableComplete();
  };
  protected writableAbort: UnderlyingSinkAbortCallback = (reason) => {
    this.streamEvents$.next(StreamState.WritableAborted);
    this.connection.connection.streamShutdown(
      this.id,
      Shutdown.Write,
      this.reasonToCode('write', reason),
    );
    this.writableAborted = true;
    this.writeReady$.next();
    this.connection.processSend();
    this.updateWritableComplete();
  };
  protected readableStart: UnderlyingSourceStartCallback<Buffer> = (
    controller,
  ) => {
    this.streamEvents$.next(StreamState.ReadableStart);
    this.readableController = controller;
  };
  protected readablePull: UnderlyingSourcePullCallback<Buffer> = async (
    controller,
  ) => {
    this.streamEvents$.next(StreamState.ReadablePullStart);
    // Attempt to read from the connection
    let chunks = 0;
    while (true) {
      const buffer = Buffer.allocUnsafe(CHUNK_SIZE);
      let result: [number, boolean] | null;
      try {
        result = this.connection.connection.streamRecv(this.id, buffer);
      } catch (e) {
        this.streamEvents$.next(StreamState.ReadableErrored);
        let error: Error;
        const code = utils.isStreamReset(e);
        if (code != null) {
          error = this.codeToReason('write', code);
        } else {
          error = new errors.ErrorQUICStreamInternal(
            `stream recv failed due to ${e.message}`,
            { cause: e },
          );
        }
        controller.error(error);
        this.updateReadableComplete();
        return;
      } finally {
        // Check if the finished state has updated to dispatch the event
        void this.isFinished;
      }
      if (result == null) {
        // If we enqueued any chunks, then we need to wait for the readableStream to drain
        if (chunks > 0) break;
        // If no chunks queued then wait for more data
        this.streamEvents$.next(StreamState.ReadableBlocked);
        await firstValueFrom(this.readReady$);
        this.streamEvents$.next(StreamState.ReadableUnblocked);
        continue;
      }
      const [bytesRead, finished] = result;
      this.streamEvents$.next(StreamState.ReadableRead);
      if (bytesRead > 0) controller.enqueue(buffer.subarray(0, bytesRead));
      chunks++;
      if (finished) {
        this.streamEvents$.next(StreamState.ReadableFinished);
        controller.close();
        this.updateReadableComplete();
        break;
      }
    }
  };

  protected readableCancel: UnderlyingSourceCancelCallback = (reason) => {
    this.streamEvents$.next(StreamState.ReadableCancelled);
    this.connection.connection.streamShutdown(
      this.id,
      Shutdown.Read,
      this.reasonToCode('read', reason),
    );
    this.readableCancelled = true;
    this.connection.processSend();
    // Cancelling doesn't immediately close the readable stream. The state will update once
    //  the cancellation has been acked and that packet processed. Annoyingly, when this happens,
    //  the stream isn't added to the readable iterator. So we need to check each time a
    //  new packet is received until it finishes.
    const subscription = this.connection.recvHandled$.subscribe(() => {
      if (this.isFinished) {
        subscription.unsubscribe();
        this.updateReadableComplete();
      }
    });
  };

  /**
   * Constructs the writableStream depending on the connection type and id.
   * If the stream is unidirectional, then the writable stream is created when
   * the ConnectionType and Initiator match
   */
  protected setupWritable(
    type: ConnectionType,
    id: StreamId,
  ): WritableStream<Buffer> {
    const isClientInitOnClient =
      type === ConnectionType.CLIENT && id % 4 === CLIENT_UNI_ID;
    const isServerInitOnServer =
      type === ConnectionType.SERVER && id % 4 === SERVER_UNI_ID;
    const isBidirectional =
      id % 4 === CLIENT_BIDI_ID || id % 4 === SERVER_BIDI_ID;
    if (isClientInitOnClient || isServerInitOnServer || isBidirectional) {
      const writableStream = new WritableStream<Buffer>({
        start: this.writableStart,
        write: this.writableWrite,
        close: this.writableClose,
        abort: this.writableAbort,
      });
      this.writeReady$.subscribe(() => {
        try {
          this.connection.connection.streamSend(
            this.id,
            Buffer.alloc(0),
            false,
          );
        } catch (e) {
          this.streamEvents$.next(StreamState.WritableErrored);
          let error: Error;
          const code = utils.isStreamStopped(e);
          if (code != null) {
            error = this.codeToReason('write', code);
          } else {
            error = new errors.ErrorQUICStreamInternal(
              `stream send failed due to ${e.message}`,
              { cause: e },
            );
          }
          this.writableController.error(error);
          this.updateWritableComplete();
        }
      });
      return writableStream;
    }

    // Otherwise initiate with the writable errored out
    const writableStream = new WritableStream({
      start: (controller) => {
        controller.error(new errors.ErrorQUICStreamUnidirectional());
      },
    });
    this.updateWritableComplete();
    return writableStream;
  }

  /**
   * Constructs the readableStream depending on the connection type and id.
   * If the stream is unidirectional, then the writable stream is created when
   * the ConnectionType and Initiator don't match
   */
  protected setupReadable(type: ConnectionType, id: StreamId) {
    const isServerInitOnClient =
      type === ConnectionType.CLIENT && id % 4 === SERVER_UNI_ID;
    const isClientInitOnServer =
      type === ConnectionType.SERVER && id % 4 === CLIENT_UNI_ID;
    const isBidirectional =
      id % 4 === CLIENT_BIDI_ID || id % 4 === SERVER_BIDI_ID;
    if (isServerInitOnClient || isClientInitOnServer || isBidirectional) {
      const readableStream = new ReadableStream<Buffer>({
        start: this.readableStart,
        pull: this.readablePull,
        cancel: this.readableCancel,
      });
      this.readReady$.subscribe(() => {
        // If the stream is finished here, then it either ended with an error or a 0-len fin packet.
        // We must do a Recv to work out which is which
        if (this.isFinished) {
          // We need to tigger a read and end the readable stream with an error
          try {
            const buf = Buffer.alloc(1024);
            const result = this.connection.connection.streamRecv(this.id, buf);
            if (result == null) {
              utils.never('got null result when expecting data');
            }
            const [bytesRead, finished] = result;
            if (!finished || bytesRead > 0) {
              utils.never('processed data when we just expected a fin frame');
            }
            this.streamEvents$.next(StreamState.ReadableFinished);
            this.readableController.close();
            this.updateReadableComplete();
            // Return;
          } catch (e) {
            this.streamEvents$.next(StreamState.ReadableErrored);
            let error: Error;
            const code = utils.isStreamReset(e);
            if (code != null) {
              error = this.codeToReason('read', code);
            } else {
              error = new errors.ErrorQUICStreamInternal(
                `stream recv failed due to ${e.message}`,
                { cause: e },
              );
            }
            this.readableController.error(error);
            this.updateReadableComplete();
            // Return;
          }
        }
      });
      return readableStream;
    }
    // Otherwise initiate with the readable errored out
    const readableStream = new ReadableStream({
      start: (controller) => {
        controller.error(new errors.ErrorQUICStreamUnidirectional());
      },
    });
    this.updateReadableComplete();
    return readableStream;
  }

  /**
   *
   * @param id - The id assigned to the stream.
   * @param connection - The connectio the stream belongs to.
   * @param codeToReason - The codeToReason callback that maps codes into errors.
   * @param reasonToCode - The ReasonToCode callback that maps errors into codes.
   */
  constructor(
    public readonly id: number,
    public readonly connection: QUICConnection,
    protected codeToReason: StreamCodeToReason,
    protected reasonToCode: StreamReasonToCode,
  ) {
    this.writable = this.setupWritable(connection.type, id);
    this.readable = this.setupReadable(connection.type, id);
    this.complete$.subscribe({
      complete: () => {
        // Complete all other subjects since no other updates should happen.
        this.readReady$.complete();
        this.writeReady$.complete();
        this.streamEvents$.complete();
        this.finished$.complete();
        this.readableComplete$.complete();
        this.writableComplete$.complete();
      },
    });
  }

  protected isFinished_ = false;
  /**
   * Emits when the underlying readable stream has finished.
   */
  public readonly finished$: ReplaySubject<void> = new ReplaySubject(1);

  /**
   * Returns true if the readable stream has finished.
   * This will only become true if all the stream data has been read from the underlying QUICConnection.
   */
  public get isFinished(): boolean {
    if (
      !this.isFinished_ &&
      this.connection.connection.streamFinished(this.id)
    ) {
      // Update and dispatch
      this.isFinished_ = true;
      this.finished$.next();
    }
    return this.isFinished_;
  }

  // Logic for stream completion
  protected _readableComplete: boolean = false;
  /**
   * Emits when the readable stream has fully completed, either via graceful finish or error.
   */
  public readonly readableComplete$: ReplaySubject<void> = new ReplaySubject();
  protected _writableComplete: boolean = false;
  /**
   * Emits when the writable stream has fully completed, either via graceful finish or error.
   */
  public readonly writableComplete$: ReplaySubject<void> = new ReplaySubject();
  protected _complete: boolean = false;
  /**
   * Emits when the QUICStream has fully completed, either via graceful finish or error.
   */
  public readonly complete$: ReplaySubject<void> = new ReplaySubject();

  protected updateReadableComplete() {
    if (this._readableComplete) return;
    this._readableComplete = true;
    this.readableComplete$.next();
    if (this._writableComplete) {
      this._complete = true;
      this.complete$.next();
      this.complete$.complete();
    }
  }

  protected updateWritableComplete() {
    if (this._writableComplete) return;
    this._writableComplete = true;
    this.writableComplete$.next();
    if (this._readableComplete) {
      this._complete = true;
      this.complete$.next();
      this.complete$.complete();
    }
  }

  /**
   * Returns true if the readable stream has fully completed, either via graceful finish or error.
   */
  get isReadableComplete() {
    return this._readableComplete;
  }

  /**
   * Returns true if the writable stream has fully completed, either via graceful finish or error.
   */
  get isWritableComplete() {
    return this._writableComplete;
  }

  /**
   * Returns true if the QUICStream has fully completed, either via graceful finish or error.
   */
  get isComplete() {
    return this._complete;
  }

  /**
   * Returns the source host of the underlying connection.
   */
  get sourceHost(): Host {
    return this.connection.sourceHost;
  }

  /**
   * Returns the source port of the underlying connection.
   */
  get sourcePort(): Port {
    return this.connection.sourcePort;
  }

  /**
   * Returns the remote host of the underlying connection.
   */
  get host(): Host {
    return this.connection.host;
  }

  /**
   * Returns the remote port of the underlying connection.
   */
  get port(): Port {
    return this.connection.port;
  }

  /**
   * Returns the remote peer certificate chain in the DER format if available.
   */
  get remoteCertChain(): Array<Uint8Array> | undefined {
    return this.connection.peerCertChain;
  }

  /**
   * Kills the writable and readable streams if they are still open.
   * The streams will be errored with the given reason. code will be generated with `reasonToCode` with the provided reason.
   * The reason defaults to `ErrorQUICStreamKilled()`.
   * @param reason - The reason for the stream to be killed.
   */
  public kill(reason: Error = new errors.ErrorQUICStreamKilled()) {
    this.streamEvents$.next(StreamState.Killed);
    // Handle killing the writable stream
    if (!this._writableComplete) {
      this.writableController.error(reason);
      this.writableAbort(reason);
    }
    // Handle killing the readable stream
    if (!this._readableComplete) {
      this.readableController.error(reason);
      this.readableCancel(reason);
      if (this.connection.isDraining || this.connection.isClosed) {
        this.streamEvents$.next(StreamState.ReadableErrored);
        this.updateReadableComplete();
      }
    }
  }
}

export default QUICStream;
