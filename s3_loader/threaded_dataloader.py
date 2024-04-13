from torch.utils.data import DataLoader
from torch.utils.data import _utils
from torch.utils.data.dataloader import _BaseDataLoaderIter, _MultiProcessingDataLoaderIter
import multiprocessing
from multiprocessing.pool import ThreadPool


def start_thread_group(args):
    with ThreadPool(ThreadedDataLoaderIter.num_threads) as pool:
        tasks = [pool.apply_async(
            func=_utils.worker._worker_loop,
            args=args
        ) for _ in range(ThreadedDataLoaderIter.num_threads)]
        for task in tasks:
            task.get()


class ThreadedDataLoaderIter(_MultiProcessingDataLoaderIter):
    num_threads = 32

    def __init__(self, loader: DataLoader):
        _BaseDataLoaderIter.__init__(self, loader)

        assert not self._pin_memory

        self._prefetch_factor = loader.prefetch_factor

        assert self._num_workers > 0
        assert self._prefetch_factor > 0

        if loader.multiprocessing_context is None:
            multiprocessing_context = multiprocessing
        else:
            multiprocessing_context = loader.multiprocessing_context

        self._worker_init_fn = loader.worker_init_fn

        # No certainty which module multiprocessing_context is
        self._worker_result_queue = multiprocessing_context.Queue()  # type: ignore[var-annotated]
        self._worker_pids_set = False
        self._shutdown = False
        self._workers_done_event = multiprocessing_context.Event()

        self._index_queues = []
        self._workers = []
        for i in range(self._num_workers):
            # No certainty which module multiprocessing_context is
            index_queue = multiprocessing_context.Queue()  # type: ignore[var-annotated]
            # Need to `cancel_join_thread` here!
            # See sections (2) and (3b) above.
            index_queue.cancel_join_thread()

            w = multiprocessing_context.Process(
                target=start_thread_group,
                args=((self._dataset_kind, self._dataset, index_queue,
                       self._worker_result_queue, self._workers_done_event,
                       self._auto_collation, self._collate_fn, self._drop_last,
                       self._base_seed, self._worker_init_fn, i, self._num_workers,
                       self._persistent_workers, self._shared_seed),))
            w.daemon = True
            # NB: Process.start() actually take some time as it needs to
            #     start a process and pass the arguments over via a pipe.
            #     Therefore, we only add a worker to self._workers list after
            #     it started, so that we do not call .join() if program dies
            #     before it starts, and __del__ tries to join but will get:
            #     AssertionError: can only join a started process.
            w.start()
            self._index_queues.append(index_queue)
            self._workers.append(w)

        self._data_queue = self._worker_result_queue

        # In some rare cases, persistent workers (daemonic processes)
        # would be terminated before `__del__` of iterator is invoked
        # when main process exits
        # It would cause failure when pin_memory_thread tries to read
        # corrupted data from worker_result_queue
        # atexit is used to shutdown thread and child processes in the
        # right sequence before main process exits
        if self._persistent_workers and self._pin_memory:
            import atexit
            for w in self._workers:
                atexit.register(_MultiProcessingDataLoaderIter._clean_up_worker, w)

        # .pid can be None only before process is spawned (not the case, so ignore)
        _utils.signal_handling._set_worker_pids(id(self), tuple(w.pid for w in self._workers))  # type: ignore[misc]
        _utils.signal_handling._set_SIGCHLD_handler()
        self._worker_pids_set = True
        self._reset(loader, first_iter=True)


class ThreadedDataLoader(DataLoader):
    def _get_iterator(self) -> _BaseDataLoaderIter:
        if self.num_workers == 0:
            return super()._get_iterator()
        else:
            self.check_worker_number_rationality()
            return ThreadedDataLoaderIter(self)