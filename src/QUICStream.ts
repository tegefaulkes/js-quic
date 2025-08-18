import type QUICConnection from './QUICConnection.js';
import type Logger from '@matrixai/logger';
import { WritableStream, ReadableStream } from 'stream/web';
import { firstValueFrom, ReplaySubject, Subject } from 'rxjs';
import * as utils from './utils.js';
import { Shutdown } from './native/index.js';

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
}

const CHECK_STATES = true;
const LOG_COMPLETE_EVENTS = true;
const LOG_STREAM_STEPS = true;
const CHUNK_SIZE = 1024;

class QUICStream {
  public readonly writable: WritableStream<Buffer>;
  public readonly readable: ReadableStream<Buffer>;
  public readonly readReady$: Subject<void> = new Subject();
  public readonly writeReady$: Subject<void> = new Subject();
  protected writableAborted = false;
  protected readableCancelled = false;
  protected readableController: ReadableStreamController<Buffer>;
  protected writableController: WritableStreamDefaultController;

  public readonly streamEvents$: Subject<StreamState> = new Subject();

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
        controller.error(e);
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
  protected writableAbort: UnderlyingSinkAbortCallback = (_reason) => {
    this.streamEvents$.next(StreamState.WritableAborted);
    this.connection.connection.streamShutdown(this.id, Shutdown.Write, 42);
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
      const result = this.connection.connection.streamRecv(this.id, buffer);
      if (result == null) {
        // If we enqueued any chunks then we need to wait for the readableStream to drain
        this.checkState();
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
      this.checkState();
      if (finished) {
        this.streamEvents$.next(StreamState.ReadableFinished);
        controller.close();
        this.updateReadableComplete();
        break;
      }
    }
  };

  protected readableCancel: UnderlyingSourceCancelCallback = () => {
    this.streamEvents$.next(StreamState.ReadableCancelled);
    this.connection.connection.streamShutdown(this.id, Shutdown.Read, 42);
    this.readableCancelled = true;
    this.connection.processSend();
    // Cancelling doesn't immedietly close the readable stream. The state will update once
    //  the cancellation has been acked and that packet processed. Annoyingly when this happens
    //  the stream isn't added to the readable iterator. So we need to check each time a
    //  new packet is received until it finishes.
    const subscription = this.connection.recvHandled$.subscribe(() => {
      if (this.isFinished) {
        subscription.unsubscribe();
        this.updateReadableComplete();
      }
    });
  };

  constructor(
    public readonly id: number,
    public readonly connection: QUICConnection,
    protected logger: Logger,
  ) {
    if (LOG_STREAM_STEPS) {
      this.streamEvents$.subscribe((v) => {
        this.logger.warn(`stream event ${StreamState[v]}`);
      });
    }
    this.writable = new WritableStream<Buffer>({
      start: this.writableStart,
      write: this.writableWrite,
      close: this.writableClose,
      abort: this.writableAbort,
    });
    this.readable = new ReadableStream<Buffer>({
      start: this.readableStart,
      pull: this.readablePull,
      cancel: this.readableCancel,
    });
    this.readReady$.subscribe(() => {
      if (CHECK_STATES) this.logger.warn(`read ready ${this.id}`);
      // If the stream is finished here then it either ended with an error or a 0-len fin packet.
      // We must do a Recv to work out which is which
      const finished = this.connection.connection.streamFinished(this.id);
      if (finished) {
        this.logger.warn(`finished ${this.id}`);
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
          return;
        } catch (_e) {
          // TODO: proper error.
          this.streamEvents$.next(StreamState.ReadableErrored);
          this.readableController.error(new Error('Stream finished'));
          this.updateReadableComplete();
          return;
        }
      }
    });
    this.writeReady$.subscribe(() => {
      if (CHECK_STATES) this.logger.warn(`write ready ${this.id}`);
      try {
        this.connection.connection.streamSend(this.id, Buffer.alloc(0), false);
      } catch (e) {
        this.logger.error(`writable failed with ${e.message}`);
        this.writableController.error(e);
        this.updateWritableComplete();
      }
    });

    if (CHECK_STATES) {
      this.readable$.subscribe(() =>
        this.logger.warn(`CHANGED readable$ ${this.id}`),
      );
    }
    if (CHECK_STATES) {
      this.writable$.subscribe(() =>
        this.logger.warn(`CHANGED writable$ ${this.id}`),
      );
    }
    if (CHECK_STATES) {
      this.finished$.subscribe(() =>
        this.logger.warn(`CHANGED finished$ ${this.id}`),
      );
    }
    if (LOG_COMPLETE_EVENTS) {
      this.readableComplete$.subscribe(() =>
        this.logger.warn(`CHANGED readableComplete$ ${this.id}`),
      );
    }
    if (LOG_COMPLETE_EVENTS) {
      this.writableComplete$.subscribe(() =>
        this.logger.warn(`CHANGED writableComplete$ ${this.id}`),
      );
    }
    if (LOG_COMPLETE_EVENTS) {
      this.complete$.subscribe(() =>
        this.logger.warn(`CHANGED complete$ ${this.id}`),
      );
    }
  }

  protected checkState() {
    void this.isFinished;
    void this.isReadable;
    void this.isWritable;
  }

  protected isFinished_ = false;
  public readonly finished$: ReplaySubject<void> = new ReplaySubject(1);
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

  protected isReadable_ = false;
  public readonly readable$: ReplaySubject<boolean> = new ReplaySubject(1);
  public get isReadable(): boolean {
    const value = this.connection.connection.streamReadable(this.id);
    if (this.isReadable_ !== value) {
      // Update and dispatch
      this.readable$.next(value);
    }
    this.isReadable_ = value;
    return value;
  }

  protected isWritable_ = false;
  public readonly writable$: ReplaySubject<boolean> = new ReplaySubject(1);
  public get isWritable(): boolean {
    const value = this.connection.connection.streamReadable(this.id);
    if (this.isWritable_ !== value) {
      // Update and dispatch
      this.writable$.next(value);
    }
    this.isWritable_ = value;
    return value;
  }

  // Logic for stream completion
  protected _readableComplete: boolean = false;
  public readonly readableComplete$: ReplaySubject<void> = new ReplaySubject();
  protected _writableComplete: boolean = false;
  public readonly writableComplete$: ReplaySubject<void> = new ReplaySubject();
  protected _complete: boolean = false;
  public readonly complete$: ReplaySubject<void> = new ReplaySubject();

  get readableComplete() {
    return this._readableComplete;
  }
  get writableComplete() {
    return this._writableComplete;
  }
  get complete() {
    return this._complete;
  }

  protected updateReadableComplete() {
    if (this._readableComplete) return;
    this._readableComplete = true;
    this.readableComplete$.next();
    this.readableComplete$.complete();
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
    this.writableComplete$.complete();
    if (this._readableComplete) {
      this._complete = true;
      this.complete$.next();
      this.complete$.complete();
    }
  }
}

export default QUICStream;
