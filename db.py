#db.py

import os,re,sys,time,uuid,socket,datetime,functools,threading,logging,collections
from utils import Dict
def next_str(t=None):
	if t is None:
		t = time.time()
	return '%015d%s000' %(int(t*1000),uuid.uuid4().hex)

next_id = next_str

def _profiling(start,sql=''):
	t = time.time() -  start
	if t > 0.1:
		logging.warning('[PROFILING] [DB] %s: %s' %(t,sql))
	else:
		logging.info('[PROFILING] [DB] %s: %s' %(t,sql))

class  DBError(Exception):
	pass

class MultiColumnsError(DBError):
	pass

def _log(s):
	logging.debug(s)
		
def _dummy_connect():
	raise DBError('Database is not initialized. call init(dbn,...) first')
_db_connect = _dummy_connect
_db_convert = ?

class  _LasyConnection(object):

	def __init__(self):
		self.connection = None

	def cursor(self):
		if self.connection is None:
			_log('open connection...')
			self.connection = _db_connect()
		return self.connection.cursor()

	def commit(self):
		self.connection.commit()

	def rollback(self):
		self.connection.rollback()

	def cleanup(self):
		if self.connection:
			connection = self.connection
			self.connection = None
			_log('close connection...')
			connection.close()
		
class _Engine(object):
	def __init__(self,connect):
		self._connect = connect
	def connect(self):
		return self._connect()
engine = None

class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0

	def is_init(self):
		return not self.connection is None

	def init (self):
		_log('open lazy connection..')
		self.connection = _LasyConnection()
		self.transactions = 0

	def cleanup(self):
		self.connection.cleanup()
		

	def cursor(self):
		return self.connection.cursor()
		
_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_cleanup = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup=True
		return self

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		if self.should_cleanup:
			_db_ctx.cleanup()

def  connection():
	return _ConnectionCtx()
def with_Connection(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			return func(*args,**kw)
	return _wrapper
class  _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn =True
		_db_ctx.transactions = _db_ctx.transactions +1
		-log('begin transactions...' if _db_ctx.transactions==1 else 'join current transactions...')
		return self		
	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions -1
		try:
			if _db_ctx.transactions ==0:
				if exctype is None:
					self.commit()
				else:
					self.rollback()

		finally:
			if self.should_close_conn:
							_db_ctx.cleanup()
	def commit(self):
		global _db_ctx
		_log('commit transactions...')
		try:
			_db_ctx.connection.commit()
			_log('commit ok.')
		except:
			logging.warning('commit failed. try rollback')
			_db_ctx.connection.rollback()
			logging.warning('rollback ok.')
			raise
	def rollback(self):
		global _db_ctx
		_log('manully rollback transaction...')
		_db_ctx.connection.rollback()
		logging.info('rollback ok.')

def transaction():
	return _TransactionCtx()

def with_Transaction():
	@functools.wraps(func)
	def _wrapper(*args,**kw)
	_start = time.time()
	with _TransactionCtx():
		return func(*args,**kw)
	_profiling(_start)
	return _wrapper

def _select(sql,first,*args):
	'execute select SQL and return unqique result or list result.'
	global _db_ctx,_db_convert




			