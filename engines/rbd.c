/*
 * CEPH RBD io engine
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>

#include <rbd/librbd.h>

#define AIO_EVENT_LOCKING
#ifdef AIO_EVENT_LOCKING
#include <pthread.h>
#endif

#include "../fio.h"


struct rbd_data {
        rados_t cluster;
	rados_ioctx_t io_ctx;
	rbd_image_t image;
	struct io_u **aio_events;
	unsigned int queued;
	uint32_t events;
#ifdef AIO_EVENT_LOCKING
	pthread_mutex_t aio_event_lock;
	pthread_cond_t aio_event_cond;
#endif
};

struct rbd_options {
	struct thread_data *td;
        char *rbd_name;
	char *pool_name;
	char *client_name;
};

static struct fio_option options[] = {
	{
		.name	  = "rbdname",
		.lname	  = "rbd engine rbdname",
		.type	  = FIO_OPT_STR_STORE,
		.help	  = "RBD name for RBD engine",
		.off1	  = offsetof(struct rbd_options, rbd_name),
		.category = FIO_OPT_C_ENGINE,
		.group	  = FIO_OPT_G_RBD,
	},
	{
		.name	  = "pool",
		.lname	  = "rbd engine pool",
		.type	  = FIO_OPT_STR_STORE,
		.help	  = "Name of the pool hosting the RBD for the RBD engine",
		.off1	  = offsetof(struct rbd_options, pool_name),
		.category = FIO_OPT_C_ENGINE,
		.group	  = FIO_OPT_G_RBD,
	},
	{
		.name	  = "clientname",
		.lname	  = "rbd engine clientname",
		.type	  = FIO_OPT_STR_STORE,
		.help	  = "Name of the ceph client to access the RBD for the RBD engine",
		.off1	  = offsetof(struct rbd_options, client_name),
		.category = FIO_OPT_C_ENGINE,
		.group	  = FIO_OPT_G_RBD,
	},
	{
		.name	= NULL,
	},
};


static int _fio_rbd_connect(struct thread_data *td)
{
	struct rbd_data *rbd_data = td->io_ops->data;
	struct rbd_options *o = td->eo;
	int r;

        dprint(FD_IO, "%s\n", __func__);

	r = rados_create(&(rbd_data->cluster), o->client_name);
	if (r < 0) {
		log_err("rados_create failed.\n");
		goto failed_early;
	}
	
	r = rados_conf_read_file(rbd_data->cluster, NULL);
	if (r < 0) {
		log_err("rados_conf_read_file failed.\n");
		goto failed_early;
	}

	r = rados_connect(rbd_data->cluster);
	if (r < 0) {
		log_err("rados_connect failed.\n");
		goto failed_shutdown;
	}

	r = rados_ioctx_create(rbd_data->cluster, o->pool_name, &(rbd_data->io_ctx));
	if (r < 0) {
		log_err("rados_ioctx_crate failed.\n");
		goto failed_shutdown;
	}
	
	r = rbd_open(rbd_data->io_ctx, o->rbd_name, &(rbd_data->image), NULL /*snap*/);
	if (r < 0) {
		log_err("rbd_open failed.\n");
		goto failed_open;
	}
	return 0;

failed_open:
	rados_ioctx_destroy(rbd_data->io_ctx);
failed_shutdown:
	rados_shutdown(rbd_data->cluster);
failed_early:
	return 1;
}

static void _fio_rbd_disconnect(struct thread_data *td)
{
	struct rbd_data *rbd_data = td->io_ops->data;

        dprint(FD_IO, "%s\n", __func__);

	if (!rbd_data)
		return;

	/* shutdown everything */
	if (rbd_data->image) {
		rbd_close(rbd_data->image);
		rbd_data->image = NULL;
	}

	if (rbd_data->io_ctx) {
		rados_ioctx_destroy(rbd_data->io_ctx);
		rbd_data->io_ctx = NULL;
	}

	if (rbd_data->cluster) {
		rados_shutdown(rbd_data->cluster);
		rbd_data->cluster = NULL;
	}
}

static void _fio_rbd_finish_write_aiocb(rbd_completion_t comp, void *data) {
	struct io_u *io_u = (struct io_u *) data;
	struct rbd_data *rbd_data = (struct rbd_data *) io_u->engine_data;
	int ret = rbd_aio_get_return_value(comp);
	dprint(FD_IO, "%s - aio write return: %d\n", __func__, ret);
        dprint(FD_IO, "%s: rbd_data: %p\n", __func__, rbd_data);
        dprint(FD_IO, "%s: rbd_completion_t: %p\n", __func__, comp);

	//rbd_aio_wait_for_complete(comp);

	ret = rbd_aio_get_return_value(comp);
	dprint(FD_IO, "%s - aio write return: %d\n", __func__, ret);
	
	rbd_aio_release(comp);
	/* TODO handle error */


#ifdef AIO_EVENT_LOCKING
	pthread_mutex_lock(&(rbd_data->aio_event_lock));
#endif
	dprint(FD_IO, "%s:  aio_events[%d] = %p\n", __func__, rbd_data->events, io_u);
	rbd_data->aio_events[rbd_data->events] = io_u;
	rbd_data->events++;

#ifdef AIO_EVENT_LOCKING
	pthread_cond_signal(&(rbd_data->aio_event_cond));
	pthread_mutex_unlock(&(rbd_data->aio_event_lock));
#endif

	rbd_data->queued--;

	return;
}


static void _fio_rbd_finish_read_aiocb(rbd_completion_t comp, void *data) {
	int ret = rbd_aio_get_return_value(comp);
	struct io_u *io_u = (struct io_u *) data;
	struct rbd_data *rbd_data = (struct rbd_data *) io_u->engine_data;
	dprint(FD_IO, "%s - aio read return: %d\n", __func__, ret);
        dprint(FD_IO, "%s: rbd_data: %p\n", __func__, rbd_data);

	/* TODO: if read needs to be veriried - we should not release comp here
		without fetching the result */
	rbd_aio_release(comp);

#ifdef AIO_EVENT_LOCKING
	pthread_mutex_lock(&(rbd_data->aio_event_lock));
#endif
	dprint(FD_IO, "%s:  aio_events[%d] = %p\n", __func__, rbd_data->events, io_u);
	rbd_data->aio_events[rbd_data->events] = io_u;
	rbd_data->events++;

#ifdef AIO_EVENT_LOCKING
	pthread_cond_signal(&(rbd_data->aio_event_cond));
	pthread_mutex_unlock(&(rbd_data->aio_event_lock));
#endif

	rbd_data->queued--;


	/* TODO handle error */

	return;
}

/*
 * The core of the module is identical to the ones included with fio,
 * read those. You cannot use register_ioengine() and unregister_ioengine()
 * for external modules, they should be gotten through dlsym()
 */

/*
 * The ->event() hook is called to match an event number with an io_u.
 * After the core has called ->getevents() and it has returned eg 3,
 * the ->event() hook must return the 3 events that have completed for
 * subsequent calls to ->event() with [0-2]. Required.
 */
static struct io_u *fio_rbd_event(struct thread_data *td, int event)
{
	struct rbd_data *rbd_data = td->io_ops->data;
        dprint(FD_IO, "%s: event:%d\n", __func__, event);

	return rbd_data->aio_events[event];
}

/*
 * The ->getevents() hook is used to reap completion events from an async
 * io engine. It returns the number of completed events since the last call,
 * which may then be retrieved by calling the ->event() hook with the event
 * numbers. Required.
 */
static int fio_rbd_getevents(struct thread_data *td, unsigned int min,
				  unsigned int max, struct timespec *t)
{
	struct rbd_data *rbd_data = td->io_ops->data;
	int events = 0;

        dprint(FD_IO, "%s\n", __func__);

#ifdef AIO_EVENT_LOCKINGx
	pthread_mutex_lock(&(rbd_data->aio_event_lock));
#endif

	while (rbd_data->events < min) {
#ifdef AIO_EVENT_LOCKING
		pthread_cond_wait(&(rbd_data->aio_event_cond), &(rbd_data->aio_event_lock));
#else
		usleep(10000);
#endif
	}



	events = rbd_data->events;
	rbd_data->events -= events;

#ifdef AIO_EVENT_LOCKINGx
	pthread_mutex_unlock(&(rbd_data->aio_event_lock));
#endif


	return events;
}

/*
 * The ->cancel() hook attempts to cancel the io_u. Only relevant for
 * async io engines, and need not be supported.
 */
static int fio_rbd_cancel(struct thread_data *td, struct io_u *io_u)
{

        dprint(FD_IO, "%s\n", __func__);
	return 0;
}

/*
 * The ->queue() hook is responsible for initiating io on the io_u
 * being passed in. If the io engine is a synchronous one, io may complete
 * before ->queue() returns. Required.
 *
 * The io engine must transfer in the direction noted by io_u->ddir
 * to the buffer pointed to by io_u->xfer_buf for as many bytes as
 * io_u->xfer_buflen. Residual data count may be set in io_u->resid
 * for a short read/write.
 */
static int fio_rbd_queue(struct thread_data *td, struct io_u *io_u)
{
	int r = -1;
	struct rbd_data *rbd_data = td->io_ops->data;
	rbd_completion_t comp;

        dprint(FD_IO, "%s\n", __func__);
	/*
	 * Double sanity check to catch errant write on a readonly setup
	 */
	fio_ro_check(td, io_u);

	/*
	 * Could return FIO_Q_QUEUED for a queued request,
	 * FIO_Q_COMPLETED for a completed request, and FIO_Q_BUSY
	 * if we could queue no more at this point (you'd have to
	 * define ->commit() to handle that.
	 */

        dprint(FD_IO, "%s: rbd_data: %p\n", __func__, rbd_data);
	io_u->engine_data = rbd_data;
	if (io_u->ddir == DDIR_WRITE) {
		dprint(FD_IO, "%s: DDIR_WRITE: io_u: %p\n", __func__, io_u);
		r = rbd_aio_create_completion(io_u, (rbd_callback_t) _fio_rbd_finish_write_aiocb, &comp);
		if (r < 0) {
		    log_err("rbd_aio_create_completion for DDIR_WRITE failed.\n");
		    goto failed;
		}
		
		r = rbd_aio_write(rbd_data->image, io_u->offset, io_u->xfer_buflen, io_u->xfer_buf, comp);
		if (r < 0) {
			log_err("rbd_aio_write failed.\n");
			goto failed;
		}
		
	} else if (io_u->ddir == DDIR_READ) {
		dprint(FD_IO, "%s: DDIR_READ\n", __func__);
		r = rbd_aio_create_completion(io_u, (rbd_callback_t) _fio_rbd_finish_read_aiocb, &comp);
		if (r < 0) {
		    log_err("rbd_aio_create_completion for DDIR_READ failed.\n");
		    goto failed;
		}
		
		r = rbd_aio_read(rbd_data->image, io_u->offset, io_u->xfer_buflen, io_u->xfer_buf, comp);

		if (r < 0) {
			log_err("rbd_aio_read failed.\n");
			goto failed;
		}
		
#if 0
	} else if (io_u->ddir == DDIR_TRIM) {
		/* TODO */
#endif
	} else {
		dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__, io_u->ddir);
#if 0
		if (rbd_data->queued)
			return FIO_Q_BUSY;
#endif

		return FIO_Q_COMPLETED;
	}

        dprint(FD_IO, "%s: comp: %p io_u: %p\n", __func__, comp, io_u);

	rbd_data->queued++;
	return FIO_Q_QUEUED;

failed:

	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

/*
 * The ->prep() function is called for each io_u prior to being submitted
 * with ->queue(). This hook allows the io engine to perform any
 * preparatory actions on the io_u, before being submitted. Not required.
 */
static int fio_rbd_prep(struct thread_data *td, struct io_u *io_u)
{

	struct rbd_data *rbd_data = td->io_ops->data;

        dprint(FD_IO, "%s: rbd_data: %p\n", __func__, rbd_data);

	io_u->engine_data = rbd_data;
	assert((io_u->flags & IO_U_F_FLIGHT) == 0);

	return 0;
}

/*
 * The init function is called once per thread/process, and should set up
 * any structures that this io engine requires to keep track of io. Not
 * required.
 */
static int fio_rbd_init(struct thread_data *td)
{
	int r;
	struct rbd_data *rbd_data = td->io_ops->data;

        dprint(FD_IO, "%s\n", __func__);


#ifdef AIO_EVENT_LOCKING
	pthread_mutex_init(&(rbd_data->aio_event_lock), NULL);
	pthread_cond_init(&(rbd_data->aio_event_cond), NULL);
#endif

	r = _fio_rbd_connect(td);
	if (r < 0) {
		log_err("fio_rbd_connect failed.\n");
		goto failed;
	}

	return 0;
failed:
	_fio_rbd_disconnect(td);
	return 1;

}

/*
 * This is paired with the ->init() function and is called when a thread is
 * done doing io. Should tear down anything setup by the ->init() function.
 * Not required.
 */
static void fio_rbd_cleanup(struct thread_data *td)
{
	struct rbd_data *rbd_data = td->io_ops->data;
        dprint(FD_IO, "%s\n", __func__);


	if (rbd_data) {
		_fio_rbd_disconnect(td);
		free(rbd_data->aio_events);
		free(rbd_data);
	}

}

static int fio_rbd_setup(struct thread_data *td)
{
	int r = 0;
	int major, minor, extra;
	rbd_image_info_t info;
	struct fio_file *f;
	struct rbd_data *rbd_data;

	// check if td->io_ops->data is already set?
	rbd_data = malloc(sizeof(struct rbd_data));
	memset(rbd_data, 0, sizeof(struct rbd_data));
	rbd_data->aio_events = malloc(td->o.iodepth * sizeof(struct io_u *));
	memset(rbd_data->aio_events, 0, td->o.iodepth * sizeof(struct io_u *));
	td->io_ops->data = rbd_data;

	/* librbd does not allow to run first in the main thread and later in a fork child */
	td->o.use_thread = 1;

	rbd_version(&major, &minor, &extra);
	log_info("rbd engine: RBD version: %d.%d.%d\n", major, minor, extra);

	r = _fio_rbd_connect(td);
	if (r < 0) {
		log_err("fio_rbd_connect failed.\n");
		goto cleanup;
	}

	r = rbd_stat(rbd_data->image, &info, sizeof(info));
	if (r < 0) {
		log_err("rbd_status failed.\n");
		goto disconnect;
	}
        dprint(FD_IO, "rbd-engine: image size: %lu\n", info.size);

	if (!td->files_index) {
		add_file(td, td->o.filename ?: "rbd");
		td->o.nr_files = td->o.nr_files ?: 1;
	}
	f = td->files[0];
	f->real_file_size = info.size;

disconnect:
	_fio_rbd_disconnect(td);
	return r;
cleanup:
	fio_rbd_cleanup(td);
	return r;
}


/*
 * Hook for opening the given file. Unless the engine has special
 * needs, it usually just provides generic_open_file() as the handler.
 */
static int fio_rbd_open(struct thread_data *td, struct fio_file *f)
{
	//struct rbd_data *rbd_data = td->io_ops->data;

        dprint(FD_IO, "%s\n", __func__);

	return 0; 
}

/*
 * Hook for closing a file. See fio_rbd_open().
 */
static int fio_rbd_close(struct thread_data *td, struct fio_file *f)
{

        dprint(FD_IO, "%s\n", __func__);
	return 0;
}


/*
 * Note that the structure is exported, so that fio can get it via
 * dlsym(..., "ioengine");
 */
struct ioengine_ops ioengine = {
	.name			= "rbd",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_rbd_setup,
	.init			= fio_rbd_init,
	.prep			= fio_rbd_prep,
	.queue			= fio_rbd_queue,
	.cancel			= fio_rbd_cancel,
	.getevents		= fio_rbd_getevents,
	.event			= fio_rbd_event,
	.cleanup		= fio_rbd_cleanup,
	.open_file		= fio_rbd_open,
	.close_file		= fio_rbd_close,
	.options		= options,
	.option_struct_size	= sizeof(struct rbd_options),
//	.flags          	= FIO_PIPEIO,
};

static void fio_init fio_rbd_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_rbd_unregister(void)
{
	unregister_ioengine(&ioengine);
}
