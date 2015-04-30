<?php
/**
 * php redis客户端，采用sentinel方式部署redis
 * key分配算法采用hash一致性算法
 *
 * 2015.3.12 18:22
 * @author enze.wei <[enzewei@gmail.com]>
 */

/**
 * 使用lib_redis实现php的redis client，分布式采用哈希一致性
 *
 * 同时实现环境选择接口
 *
 * @example
 * 
 * $redis = new RedisDB(group);	//master
 * $redisRead = new RedisDB(group); //slave
 */
class RedisDB {

	const TIME_OUT = 3;

	/*
	 * 300 second
	 */
	const EXPIRE = 3000;

	private $_redis = null;
	private $_sentinel = null;
	private $_conn = array();

	private $_servers = array();

	private $_config = array();

	private $_group = null;

	public function __construct($group) {
		$this->_init($group);
	}

	private function _init($group) {
		if (true === empty($this->_config)) {
			$config = Loader::loadConfig('redis');
			$this->_config = $config[$group];
		}

		if (null === $this->_sentinel) {
			$this->_sentinel = new SentinelMonitor;
		}

		if (true === empty($this->_sentinel->servers)) {
			$this->_sentinel->addServers($this->_config);
		}

		$this->_group = $group;
	}

	//1 slave 2 master
	private function _connect($method = 1) {
		switch ($method) {
			case 2:
				if (true === empty($this->_conn[$this->_group]['master'])) {
					$host = $this->_sentinel->getMasterServer();
					$this->_conn[$this->_group]['master'] = new Redis;
					$this->_conn[$this->_group]['master']->connect($host[0], $host[1], self::TIME_OUT);
					$this->_servers['host'] = $host[0];
					$this->_servers['port'] = $host[1];
					$this->_conn[$this->_group]['master']->auth($host[2]);
					$this->_conn[$this->_group]['master']->select($host[3]);
				}
				break;
			case 1:
			default:
				if (true === empty($this->_conn[$this->_group]['slave'])) {
					$host = $this->_sentinel->getSlaveServer();
					$this->_conn[$this->_group]['slave'] = new Redis;
					$this->_conn[$this->_group]['slave']->connect($host[0], $host[1], self::TIME_OUT);
					$this->_servers['host'] = $host[0];
					$this->_servers['port'] = $host[1];
					$this->_conn[$this->_group]['slave']->auth($host[2]);
					$this->_conn[$this->_group]['slave']->select($host[3]);
				}
				break;
		}
	}

	private function _encode($data) {
		return json_encode($data);
	}

	private function _decode($data) {
		return json_decode($data, true);
	}

	private function _write($key) {
		$this->_sentinel->key = $key;
		if (true === empty($this->_conn)) {
			$this->_connect(2);
		}
	}

	private function _read($key) {
		$this->_sentinel->key = $key;
		if (true === empty($this->_conn)) {
			$this->_connect();
		}
	}

	/********************************* Redis Command *************************/

	/****** about keys *******/

	/**
	 * 删除一个key
	 * @param  string $key
	 * @return boolean [true|false]
	 */
	public function del($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$result = $this->_conn[$this->_group]['master']->del($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('del key:{' . $key . '} return {' . (int) $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 获取指定key的存活周期，单位毫秒
	 * @param  string $key
	 * @return long [-1 no ttl|-2 key doesn't exist|numric]
	 */
	public function pttl($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$result = $this->_conn[$this->_group]['slave']->pttl($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('pttl key:{' . $key . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 获取指定key的存活周期，单位秒
	 * @param  string $key
	 * @return long [-1 no ttl|-2 key doesn't exist|numric]
	 */
	public function ttl($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$result = $this->_conn[$this->_group]['slave']->ttl($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('ttl key:{' . $key . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 检察某一个key是否存在
	 * @param  string $key
	 * @return boolean [true|false]
	 */
	public function exists($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$result = $this->_conn[$this->_group]['slave']->exists($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('exists key:{' . $key . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 返回key所存储的值的类型
	 * @param  string $key
	 * @return int [none|string|list|set|zset|hash]
	 *
	 * string: Redis::REDIS_STRING  1
	 * set: Redis::REDIS_SET  2
	 * list: Redis::REDIS_LIST  3
	 * zset: Redis::REDIS_ZSET  4
	 * hash: Redis::REDIS_HASH  5
	 * other: Redis::REDIS_NOT_FOUND  0
	 */
	public function type($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$result = $this->_conn[$this->_group]['slave']->type($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('type key:{' . $key . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 设置key的过期时间。
	 * 如果key已过期，将会被自动删除。设置了过期时间的key被称之为volatile。
	 * 在key过期之前可以重新更新他的过期时间，也可以使用PERSIST命令删除key的过期时间。
	 * @param  string $key
	 * @param  integer $expire
	 * @return boolean
	 */
	public function expire($key, $expire = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$expire = intval($expire);
		$result = $this->_conn[$this->_group]['master']->expire($key, $expire);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('expire key:{' . $key . '} expire {' . $expire . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 移除一个key的有效期，但并不删除该key，使之永不失效
	 * @param  string $key
	 * @return boolean [true|false]
	 */
	public function persist($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$result = $this->_conn[$this->_group]['master']->persist($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('persist key:{' . $key . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 设置key的过期时间。 时间戳
	 * 如果key已过期，将会被自动删除。设置了过期时间的key被称之为volatile。
	 * 在key过期之前可以重新更新他的过期时间，也可以使用PERSIST命令删除key的过期时间。
	 * @param  string $key
	 * @param  integer $timestamp
	 * @return boolean
	 */
	public function expireat($key, $timestamp = 0) {
		$startTime = RpcLog::getMicroTime();
		if (true === empty($timestamp)) {
			$timestamp = time() + self::EXPIRE;
		}
		$this->_write($key);
		$result = $this->_conn[$this->_group]['master']->expireat($key, $timestamp);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('expireat key:{' . $key . '} timestamp {' . $timestamp . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 设置key的过期时间。毫秒
	 * 如果key已过期，将会被自动删除。设置了过期时间的key被称之为volatile。
	 * 在key过期之前可以重新更新他的过期时间，也可以使用PERSIST命令删除key的过期时间。
	 * @param  string $key
	 * @param  integer $expire
	 * @return boolean
	 */
	public function pexpire($key, $expire = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$expire = intval($expire);
		$result = $this->_conn[$this->_group]['master']->pexpire($key, $expire);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('pexpire key:{' . $key . '} expire {' . $expire . '} return {' . $result . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $result;
	}

	/**
	 * 排序
	 * 
	 * 该处推荐使用场景如下：
	 * 1、简单的数字、字母的value，并且value是单一的值，不是序列化的字串等形式，直接sort加排序方式即可，根据需要可增加alpha、limit修饰
	 * 2、有序集合，可根据by进行排序。
	 * 其他场景不推荐使用，如根据by进行外部key的权重排序，因为外部key不一定与当前的server是同一个server
	 * 因此推荐列表页的数据都存储到有序集合中，然后针对该集合排序
	 * 
	 * @param  string $key
	 * @param  array  $option 配置参数，格式参考示例
	 * @return array
	 *
	 * @example
	 * $option = array(
	 * 				'by' => 'field',  //需要排序的字段
	 * 				'limit' => array(startIndex, offset), //起始位置，一次取多少条数据，与mysql中的limit类似
	 * 				'sort' => 'asc|desc',
	 * 				'alpha' => true|false, //是否采用alpha排序
	 * 				'store' => 'new_key', //排序结果存储到指定的key中，alert，当前模式下，不能保证在read的时候会选择存储的机器，因此不推荐该方式
	 * );
	 */
	public function sort($key, $option = array()) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		if (true === empty($option)) {
			$result = $this->_conn[$this->_group]['master']->sort($key);
			$command = 'sort use default key:{' . $key . '}';
		} else {
			$result = $this->_conn[$this->_group]['master']->sort($key, $option);
			$command = 'sort use option key:{' . $key . '} option:{' . json_encode($option) . '}';
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log($command . ' return {' . json_encode($result) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);

		if (true === is_array($result)) {
			foreach ($result as $key => $value) {
				$result[$key] = $this->_decode($value);
			}
		}
		return $result;
	}

	/**
	 * 移动一个key到另一个数据库
	 * 当使用过该方法后，以后的查询必须做selectDb操作
	 * 
	 * @param  string $key
	 * @param  integer $dbNumber db数量，最大不超过配置的数量，默认是15 (从0开始)
	 * @return boolean
	 */
	public function move($key, $dbNumber = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->move($key, $dbNumber);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('move key:{' . $key . '} dbnumber:{' . $dbNumber . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/****** about strings *******/

	/**
	 * 根据key获取值
	 * @param  string $key
	 * @return mix
	 */
	public function get($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$data = $this->_conn[$this->_group]['slave']->get($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('get key:{' . $key . '} value:{' . $data . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		$data = $this->_decode($data);
		return $data;
	}

	/**
	 * 设置key的值
	 * 第三个参数有几种用法，这里贴出在php的redis扩展中C代码是怎么实现的
	 * 
	 * @param [type]  $key    [description]
	 * @param [type]  $data   [description]
	 * @param mix $option [empty|numeric|array] 该参数分别对应几种设置方法  [NX以及XX暂不支持，有问题]
	 *
	 * redis 2.6.12 support extend third args (array)
	 * 
	 * @tips php_method set的c代码实现
	 * if(exp_type && set_type) {
	 * 		//SET <key> <value> NX|XX PX|EX <timeout> 
	 *		cmd_len = redis_cmd_format_static(&cmd, "SET", "ssssl", key, key_len, *	val, val_len, set_type, 2, exp_type, * 2, expire);
	 *	} else if(exp_type) {
	 * 		//SET <key> <value> PX|EX <timeout> 
	 *	  	cmd_len = redis_cmd_format_static(&cmd, "SET", "sssl", key, key_len, * val, val_len, exp_type, 2, expire);
	 *	} else if(set_type) {
	 *	 	//SET <key> <value> NX|XX 
	 *   	cmd_len = redis_cmd_format_static(&cmd, "SET", "sss", key, key_len, * val, val_len, set_type, 2);
	 *	} else if(expire > 0) {
	 * 		//Backward compatible SETEX redirection 
	 *	  	cmd_len = redis_cmd_format_static(&cmd, "SETEX", "sls", key, key_len, *	expire, val, val_len);
	 *	} else {
	 *	 	//SET <key> <value> 
	 *	  	cmd_len = redis_cmd_format_static(&cmd, "SET", "ss", key, key_len, * val, val_len);
	 *	}
	 */
	public function set($key, $data, $option = array()) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		if (true === empty($option)) {
			/*
			 * default
			 */
			$res = $this->_conn[$this->_group]['master']->set($key, $data);
			$command = 'set';
		} else if (true === is_numeric($option)) {
			/*
			 * use setex default second
			 */
			$res = $this->_conn[$this->_group]['master']->set($key, $data, (int) $option);
			$command = 'set expire:' . $option . 's';
		} else if (true === is_array($option)) {
			/*
			 * option  array(
			 * 				'type' => 'NX|XX|PX|EX',
			 * 				'expire' => array('PX|EX' => 1000)|1000,
			 * 			)
			 */

			try {
				switch (strtoupper($option['type'])) {
					/*
					case 'NX' :
						if (true === empty($option['expire']) || false === is_array($option['expire'])) {
							$option['expire'] = array('EX' => self::EXPIRE);
						}
						$res = $this->_conn[$this->_group]['master']->set($key, $data, (array('NX') + $option['expire']));
						$command = '(NX) set use' . json_encode($option['expire']);
						break;
					case 'XX' :
						if (true === empty($option['expire']) || false === is_array($option['expire'])) {
							$option['expire'] = array('EX' => self::EXPIRE);
						}
						$res = $this->_conn[$this->_group]['master']->set($key, $data, (array('XX') + $option['expire']));
						debug((array('XX') + $option['expire']));
						$command = '(xx) set use' . json_encode($option['expire']);
						break;
					*/
					case 'PX' :
						if (true === empty($option['expire'])) {
							$option['expire'] = self::EXPIRE;
						}
						$res = $this->_conn[$this->_group]['master']->psetex($key, (int) $option['expire'], $data);
						$command = '(px) set use psetex expire:' . $option['expire'] . 'ms';
						break;
					case 'EX' :
					default :
						if (true === empty($option['expire'])) {
							$option['expire'] = self::EXPIRE;
						}
						debug($option);
						$res = $this->_conn[$this->_group]['master']->set($key, $data, (int) $option['expire']);
						$command = '(ex) set expire ' . $option['expire'] . 's';
						break;
				}
			} catch (Exception $e) {
				throw $e;
			}
		} else {
			$res = $this->_conn[$this->_group]['master']->set($key, $data, self::EXPIRE);
			$command = '(default) set';
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log($command . ' key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 以秒设置expire
	 * @param  string  $key
	 * @param  mix  $data
	 * @param  integer $expire 生命周期
	 * @return boolean
	 */
	public function setex($key, $data, $expire = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->setex($key, (int) $expire, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('setex key:{' . $key . '} value:{' . $data . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 以毫秒设置expire
	 * @param  string  $key
	 * @param  mix  $data
	 * @param  integer $expire 生命周期
	 * @return boolean
	 */
	public function psetex($key, $data, $expire = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->psetex($key, (int) $expire, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('psetex key:{' . $key . '} value:{' . $data . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 追加一个值到指定的key
	 * @param  string  $key
	 * @param  mix  $data
	 * @return integer  当前key的值对应的长度
	 */
	public function append($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->append($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('append key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * SET if Not Exists
	 * 如果key不存在，就设置key对应字符串value。在这种情况下，该命令和SET一样。当key已经存在时，就不做任何操作。
	 * 
	 * @param  string  $key
	 * @param  mix  $data
	 * @return boolean
	 */
	public function setnx($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->setnx($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('setnx key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 自动将key对应到value并且返回原来key对应的value。如果key存在但是对应的value不是字符串，就返回错误。
	 *
	 * @tips GETSET可以和INCR一起使用实现支持重置的计数功能。举个例子：每当有事件发生的时候，
	 * 一段程序都会调用INCR给key mycounter加1，但是有时我们需要获取计数器的值，并且自动将其重置为0。
	 * 这可以通过GETSET mycounter "0"来实现
	 * 
	 * @param  string  $key
	 * @param  mix  $data
	 * @return mix
	 */
	public function getset($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		if (false === is_numeric($data)) {
			$data = $this->_encode($data);
		} else {
			$data = intval($data);
		}
		$res = $this->_conn[$this->_group]['master']->getset($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('getset key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 对key对应的数字做减1操作。
	 *
	 * @tips 如果key不存在，那么在操作之前，这个key对应的值会被置为0。
	 * 如果key有一个错误类型的value或者是一个不能表示成数字的字符串，
	 * 就返回错误。这个操作最大支持在64位有符号的整型数字。
	 * 
	 * @param  string  $key
	 * @param  int  [$offset]
	 * @return int 减小之后的value
	 */
	public function decr($key, $offset = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$offset = intval($offset);
		if (1 != $offset) {
			$res = $this->_conn[$this->_group]['master']->decrBy($key, $offset);
		} else {
			$res = $this->_conn[$this->_group]['master']->decr($key);
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('decr key:{' . $key . '} offset:{' . $offset . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 对key对应的数字做加1操作。
	 *
	 * @tips 如果key不存在，那么在操作之前，这个key对应的值会被置为0。
	 * 如果key有一个错误类型的value或者是一个不能表示成数字的字符串，
	 * 就返回错误。这个操作最大支持在64位有符号的整型数字。
	 * 
	 * @param  string  $key
	 * @param  int  [$offset]
	 * @return int 减小之后的value
	 */
	public function incr($key, $offset = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$offset = intval($offset);
		if (1 != $offset) {
			$res = $this->_conn[$this->_group]['master']->incrBy($key, $offset);
		} else {
			$res = $this->_conn[$this->_group]['master']->incr($key);
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('incr key:{' . $key . '} offset:{' . $offset . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 对key对应的数字做加float操作。
	 * 
	 * @param  string  $key
	 * @param  float  [$offset]
	 * @return int 减小之后的value
	 */
	public function incrByFloat($key, $offset) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$offset = floatval($offset);
		$res = $this->_conn[$this->_group]['master']->incrByFloat($key, $offset);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('incrByFloat key:{' . $key . '} offset:{' . $offset . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获取key对应value的长度
	 * 
	 * @param  string  $key
	 * @return int key对应value的长度
	 */
	public function strlen($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->strlen($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('strlen key:{' . $key . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/****** about hashs *******/

	/**
	 * 读取哈希里的一个字段的值
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @return mix [false失败]
	 */
	public function hGet($key, $field) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hGet($key, $field);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hget key:{' . $key . '} field:{' . $field . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 判断给定字段是否存在于哈希表中
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @return boolean [true存在|false不存在]
	 */
	public function hExists($key, $field) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hExists($key, $field);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hexists key:{' . $key . '} field:{' . $field . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 删除一个或多个哈希字段
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @return boolean [true成功|false失败]
	 */
	public function hDel($key, $field) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->hDel($key, $field);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hdel key:{' . $key . '} field:{' . $field . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 设置hash里面一个字段的值
	 * 
	 * @param  string  $key
	 * @param  string $field hash中的字段名
	 * @param  mix $data field对应的值
	 * @return mix        [false出错|0值已存在，被替换|1添加成功，并且原先不存在这个值]
	 */
	public function hSet($key, $field, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->hSet($key, $field, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hset key:{' . $key . '} field:{' . $field . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 设置hash字段值
	 * 
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  array  $data field对应的值
	 * @return boolean
	 */
	public function hMSet($key, $field = array(), $data = array()) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		foreach ($data as $k => $v) {
			$data[$k] = $this->_encode($v);
		}
		$option = array_combine($field, $data);
		$res = $this->_conn[$this->_group]['master']->hMSet($key, $option);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hmset key:{' . $key . '} field:{' . $this->_encode($field) . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获取hash里面指定字段的值
	 * 
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @return array
	 */
	public function hMGet($key, $field = array()) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hMGet($key, $field);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hMGet key:{' . $key . '} field:{' . $this->_encode($field) . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		foreach ($res as $k => $v) {
			if (false !== $v) {
				$res[$k] = $this->_decode($v);
			}
		}
		return $res;
	}

	/**
	 * 设置hash的一个字段，只有当这个字段不存在时有效
	 * 
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  array  $data field对应的值
	 * @return boolean
	 */
	public function hSetNx($key, $field, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->hSetNx($key, $field, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hsetnx key:{' . $key . '} field:{' . $field . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 将哈希集中指定字段的值增加给定的数字
	 * 
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  integer $offset
	 * @return mix [false|int]
	 */
	public function hIncrBy($key, $field, $offset = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->hIncrBy($key, $field, $offset);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hIncrBy key:{' . $key . '} field:{' . $field . '} offset:{' . $offset . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 将哈希集中指定域的值增加给定的浮点数
	 * 
	 * @param  string  $key
	 * @param  array  $field hash中的字段名
	 * @param  float  $offset
	 * @return mix [false|float]
	 */
	public function hIncrByFloat($key, $field, $offset = 1.0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->hIncrByFloat($key, $field, $offset);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hIncrByFloat key:{' . $key . '} field:{' . $field . '} offset:{' . $offset . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获得hash的所有值
	 * 
	 * @param  string  $key
	 * @return array
	 */
	public function hVals($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hVals($key);
		foreach($res as $k => $v) {
			$res[$k] = $this->_decode($v);
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hVals key:{' . $key . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获得hash的所有field
	 * 
	 * @param  string  $key
	 * @return array
	 */
	public function hKeys($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hKeys($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hKeys key:{' . $key . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获得hash的所有field与值
	 * 
	 * @param  string  $key
	 * @return array
	 */
	public function hGetAll($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hGetAll($key);
		foreach($res as $k => $v) {
			$res[$k] = $this->_decode($v);
		}
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hGetAll key:{' . $key . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获取hash里所有字段的数量
	 * @param  string  $key
	 * @return int
	 */
	public function hLen($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->hLen($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('hLen key:{' . $key . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/****** about lists *******/

	/**
	 * 将所有指定的值插入到存于 key 的列表的头部。
	 * 如果 key 不存在，那么在进行 push 操作前会创建一个空列表。 如果 key 对应的值不是一个 list 的话，那么会返回一个错误。
	 * 
	 * @param  string  $key
	 * @param  mix $data
	 * @return int       在 push 操作后的 list 长度。
	 */
	public function lPush($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->lpush($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lpush key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 将所有指定的值插入到存于 key 的列表的尾部。
	 * 如果 key 不存在，那么在进行 push 操作前会创建一个空列表。 如果 key 对应的值不是一个 list 的话，那么会返回一个错误。
	 * 
	 * @param  string  $key
	 * @param  mix $data
	 * @return int       在 push 操作后的 list 长度。
	 */
	public function rPush($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->rpush($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('rpush key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 只有当 key 已经存在并且存着一个 list 的时候，在这个 key 下面的 list 的头部插入 value。
	 * key 不尊在的时候不进行任何操作
	 * 
	 * @param  string  $key
	 * @param  mix $data
	 * @return int       在 push 操作后的 list 长度。| key不存在，则返回0
	 */
	public function lPushx($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->lpushx($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lpushx key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 将所有指定的值插入到存于 key 的列表的尾部。
	 * 如果 key 不存在，那么在进行 push 操作前会创建一个空列表。 如果 key 对应的值不是一个 list 的话，那么会返回一个错误。
	 * 
	 * @param  string  $key
	 * @param  mix $data
	 * @return int       在 push 操作后的 list 长度。
	 */
	public function rPushx($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->rpushx($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('rpushx key:{' . $key . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 弹出 key 对应的 list 的第一个元素。
	 * @param  string  $key
	 * @return mix [false空队列]
	 */
	public function lPop($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->lpop($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lpop key:{' . $key . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}

	/**
	 * 弹出 key 对应的 list 的最后一个元素。
	 * @param  string  $key
	 * @return mix [false空队列]
	 */
	public function rPop($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->rpop($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('rpop key:{' . $key . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}

	/**
	 * 弹出 key 对应的 list 的第一个元素。 (阻塞，当前模式下不支持非阻塞)
	 * @param  string  $key
	 * @return mix [false空队列|value]
	 */
	public function bLpop($key, $timeout = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->blpop($key, $timeout);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('blpop key:{' . $key . '} timeout:{' . $timeout . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res[1]);
		}
		return $res;
	}

	/**
	 * 弹出 key 对应的 list 的最后一个元素。 (阻塞，当前模式下不支持非阻塞)
	 * @param  string  $key
	 * @return mix [false空队列|value]
	 */
	public function bRpop($key, $timeout = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->brpop($key, $timeout);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('brpop key:{' . $key . '} timeout:{' . $timeout . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res[1]);
		}
		return $res;
	}

	/**
	 * 返回存储在 key 里的list的长度。
	 * @param  string  $key
	 * @return mix [false不是list|int 0空队列]
	 */
	public function lLen($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->llen($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('llen key:{' . $key . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 从存于 key 的列表里移除前 count 次出现的值为 value 的元素。
	 * count > 0: 从头往尾移除值为 value 的元素。
	 * count < 0: 从尾往头移除值为 value 的元素。
	 * count = 0: 移除所有值为 value 的元素。
	 * @param  string  $key
	 * @param mix $data 特别注意数字是否有引号，因为这里是会做json_encode的操作，当非int类型数字时，是会有引号出现的。
	 * @return int 被移除的元素个数。
	 */
	public function lRem($key, $data, $count = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->lrem($key, $data, $count);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lrem key:{' . $key . '} data:{' . $data . '} count:{' . $count . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 设置 index 位置的list元素的值为 value。
	 * @param  string  $key
	 * @param  mix $data
	 * @param  int $index 索引
	 * @return boolean
	 */
	public function lSet($key, $data, $index = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->lset($key, $index, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lset key:{' . $key . '} data:{' . $data . '} index:{' . $index . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 返回列表里的元素的索引 index 存储在 key 里面。  lindex别名
	 * 下标是从0开始索引的，所以 0 是表示第一个元素， 
	 * 1 表示第二个元素，并以此类推。 
	 * 负数索引用于指定从列表尾部开始索引的元素。
	 * 在这种方法下，-1 表示最后一个元素，
	 * -2 表示倒数第二个元素，并以此往前推。
	 * @param  string  $key
	 * @param  integer $index [description]
	 * @return mix [false|mix]
	 */
	public function lGet($key, $index = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->lget($key, $index);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lget key:{' . $key . '} index:{' . $index . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}

	/**
	 * 返回列表里的元素的索引 index 存储在 key 里面。
	 * 下标是从0开始索引的，所以 0 是表示第一个元素， 
	 * 1 表示第二个元素，并以此类推。 
	 * 负数索引用于指定从列表尾部开始索引的元素。
	 * 在这种方法下，-1 表示最后一个元素，
	 * -2 表示倒数第二个元素，并以此往前推。
	 * @param  string  $key
	 * @param  integer $index [description]
	 * @return mix [false|mix]
	 */
	public function lIndex($key, $index = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->lindex($key, $index);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('lindex key:{' . $key . '} index:{' . $index . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}

	/**
	 * 修剪到指定范围内的list
	 * @param  string  $key
	 * @param  integer $start 开始的索引
	 * @param  integer $stop  结束的索引
	 * @return boolean [true|false]
	 */
	public function lTrim($key, $start = 0, $stop = 999) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->ltrim($key, $start, $stop);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('ltrim key:{' . $key . '} start:{' . $start . '} stop:{' . $stop . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 在列表中的另一个元素之前或之后插入一个元素
	 * @param  string  $key
	 * @param  mix $pivot 特别注意数字是否有引号，因为这里是会做json_encode的操作，当非int类型数字时，是会有引号出现的。
	 * @param  mix  $data
	 * @param  integer $position 1前（Redis::BEFORE） 0后（Redis::AFTER）
	 * @return int  list总数，-1代表pivot不存在
	 */
	public function lInsert($key, $pivot, $data, $position = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$pivot = $this->_encode($pivot);
		$data = $this->_encode($data);
		switch ($position) {
			case 0:
				$position = Redis::AFTER;
				break;
			case 1:
			default:
				$position = Redis::BEFORE;
				break;
		}
		$res = $this->_conn[$this->_group]['master']->linsert($key, $position, $pivot, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('linsert key:{' . $key . '} position:{' . $position . '} pivot:{' . $pivot . '} data:{' . $data . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 从列表中获取指定返回的元素
	 * @param  string  $key
	 * @param  integer $start
	 * @param  integer $end
	 * @return array
	 */
	public function lRange($key, $start = 0, $end = -1) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->lrange($key, $start, $end);
		$endTime = RpcLog::getMicroTime();
		if (true === is_array($res)) {
			foreach ($res as $k => $v) {
				$res[$k] = $this->_decode($v);
			}
		}
		RpcLog::log('lrange key:{' . $key . '} start:{' . $start . '} end:{' . $end . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/****** about sets *******/

	/**
	 * 添加一个或者多个元素到集合(set)里
	 *
	 * data需要encode
	 *
	 * 支持 sadd(key, data, data, data, [data....])
	 * 
	 * @param  string  $key
	 * @param  string $data
	 * @return int
	 */
	public function sAdd($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$args = func_get_args();
		$res = call_user_func_array(array($this->_conn[$this->_group]['master'], 'sAdd'), $args);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('sAdd key:{' . $key . '} args:{' . $this->_encode($args) . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获取集合里面的元素数量
	 * 
	 * @param  string  $key
	 * @return int
	 */
	public function sCard($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->scard($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('scard key:{' . $key . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 移除并返回一个集合中的随机元素
	 * @param  string  $key
	 * @return mix
	 */
	public function sPop($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->spop($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('spop key:{' . $key . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}

	/**
	 * 返回成员 member 是否是存储的集合 key的成员.
	 * 
	 * @param  string  $key
	 * @param  mix $data
	 * @return boolean [true|false]
	 */
	public function sIsMember($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['slave']->sIsMember($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('sIsMember key:{' . $key . '} data:{' . $this->_encode($data) . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 从集合里面随机获取一个元素
	 * 
	 * 不使用count 参数的情况下该命令返回随机的元素,如果key不存在则返回nil . 
	 * 使用count参数,则返回一个随机的元素数组,如果key不存在则返回一个空的数组.
	 * @param  string  $key
	 * @param  integer $count
	 * @return mix     [false|array]
	 */
	public function sRandMember($key, $count = 1) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->sRandMember($key, $count);
		$endTime = RpcLog::getMicroTime();
		if (false !== $res) {
			foreach ($res as $k => $v) {
				$res[$k] = $this->_decode($v);
			}
		}
		RpcLog::log('sRandMember key:{' . $key . '} count:{' . $count . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 获取集合里面的所有元素
	 * @param  string  $key
	 * @return array
	 */
	public function sMembers($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->sMembers($key);
		$endTime = RpcLog::getMicroTime();
		if (false !== $res) {
			foreach ($res as $k => $v) {
				$res[$k] = $this->_decode($v);
			}
		}
		RpcLog::log('sMembers key:{' . $key . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 从集合里删除一个或多个元素
	 * @param  string  $key
	 * @param  mix $data  当为纯数字时无需json_encode，其他情况，请json_encode
	 * @return int [删除的个数]
	 */
	public function sRem($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$args = func_get_args();
		$res = call_user_func_array(array($this->_conn[$this->_group]['master'], 'srem'), $args);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('srem key:{' . $key . '} args:{' . $this->_encode($args) . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		if (false !== $res) {
			$res = $this->_decode($res);
		}
		return $res;
	}

	/****** about sorted sets *******/

	/**
	 * 添加到有序set的一个或多个成员，或更新的分数，如果它已经存在
	 *
	 * 支持zadd($key, $score, $data, [$score, $data] ...)
	 *
	 * data需要encode
	 * 
	 * @param  string  $key
	 * @param  float $score 分数/权重
	 * @param  string $data
	 * @return int
	 */
	public function zAdd($key, $score, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$args = func_get_args();
		$res = call_user_func_array(array($this->_conn[$this->_group]['master'], 'zadd'), $args);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zadd key:{' . $key . '} args:{' . $this->_encode($args) . '} return:{' . $res . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 返回的成员在排序设置的范围
	 * @param  string  $key
	 * @param  integer $start
	 * @param  integer $end
	 * @param  boolean $withscores 是否按照scores排序
	 * @return array
	 */
	public function zRange($key, $start = 0, $end = -1, $withscores = false) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->zrange($key, $start, $end, $withscores);
		$endTime = RpcLog::getMicroTime();
		if (true === is_array($res)) {
			foreach ($res as $k => $v) {
				$res[$k] = $this->_decode($v);
			}
		}
		RpcLog::log('zrange key:{' . $key . '} start:{' . $start . '} end:{' . $end . '} withscores:{' . $withscores . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 返回的成员在排序设置的范围
	 * @param  string  $key
	 * @param  integer $start
	 * @param  integer $end
	 * @param  array   $option array('withscores' => false|true, 'limit' => array('offset', 'count'))
	 * @return array
	 */
	public function zRangeByScore($key, $start = 0, $end = 1, $option = array()) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->zRangeByScore($key, $start, $end, $option);
		$endTime = RpcLog::getMicroTime();
		if (true === is_array($res)) {
			/*
			 * 如果withscores为true，则key为值，value为score
			 */
			if (true === $option['withscores']) {
				$i = 0;
				$data = array();
				foreach ($res as $k => $v) {
					$data[$i] = $this->_decode($k);
					$data[$i]['score'] = $v;
					$i++;
				}
				$res = $data;
			} else {
				foreach ($res as $k => $v) {
					$res[$k] = $this->_decode($v);
				}
			}
		}
		RpcLog::log('zRangeByScore key:{' . $key . '} start:{' . $start . '} end:{' . $end . '} option:{' . $this->_encode($option) . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 获取一个排序的集合中的成员数量
	 * @param  string  $key
	 * @return int
	 */
	public function zCard($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->zcard($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zcard key:{' . $key . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 给定值范围内的成员数
	 * @param  string  $key
	 * @param  integer $start
	 * @param  integer $end
	 * @return int
	 */
	public function zCount($key, $start, $end) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->zcount($key, $start, $end);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zcount key:{' . $key . '} start:{' . $start . '} end:{' . $end . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 确定在排序集合成员的索引
	 * @param  string  $key
	 * @param  mix $data 成员值
	 * @return int
	 */
	public function zRank($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['slave']->zrank($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zrank key:{' . $key . '} data:{' . $data . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 增量的一名成员在排序设置的评分
	 * @param  string  $key
	 * @param  float $offset 增量值
	 * @param  mix $data
	 * @return float
	 */
	public function zIncrBy($key, $offset, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->zIncrBy($key, $offset, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zIncrBy key:{' . $key . '} offset:{' . $offset . '} data:{' . $data . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 从排序的集合中删除一个或多个成员
	 * @param  string  $key
	 * @param  mix $data
	 * @return int
	 */
	public function zRem($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['master']->zRem($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zRem key:{' . $key . '} data:{' . $data . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/**
	 * 返回有序集key中成员member的排名，其中有序集成员按score值从大到小排列。
	 * @param  string  $key
	 * @param  mix $data
	 * @return mix [false|int]
	 */
	public function zRevRank($key, $data) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$data = $this->_encode($data);
		$res = $this->_conn[$this->_group]['slave']->zRevRank($key, $data);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('zRevRank key:{' . $key . '} data:{' . $data . '} return:{' . $this->_encode($res) . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		
		return $res;
	}

	/****** about transactions *******/

	/**
	 *  开启redis事务
	 *
	 * 当开启事务时，后续的方法直接跳出本类，请注意，需要使用master来实现事务的处理，另需要在同一台redis server中，才可以正确获取结果
	 * 也就是这里要传递key的原因，当要取消的时候，直接调用discard方法，当要执行命令的时候，直接调用exec方法，执行结果会以数组的形式返回
	 *
	 * @example
	 * $res = $redis->multi('test_key_106')->get('test_key_106')->hget('test_key_106', 'username')->smembers('test_key_101')->discard();  //should be cancle
	 * $res = $redis->multi('test_key_106')->get('test_key_106')->hget('test_key_106', 'username')->smembers('test_key_101')->exec();  //should be exec
	 * 
	 * @param  string  $key
	 * @return object
	 */
	public function multi($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->multi();
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('multi key:{' . $key . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	/**
	 * 锁定key直到执行了 MULTI/EXEC 命令
	 *
	 * @example
	 * $redis->watch('test_key_106');
	 * $redis->hIncrBy('test_key_106', 'rank', 2);
	 * $res = $redis->multi('test_key_106')->hgetall('test_key_106')->hIncrBy('test_key_106', 'rank', 1)->exec(); //false
	 * 
	 * $redis->watch('test_key_106');
	 * $res = $redis->multi('test_key_106')->hgetall('test_key_106')->hIncrBy('test_key_106', 'rank', 1)->exec(); //return array
	 * 
	 * @param  string  $key
	 * @return void
	 */
	public function watch($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$this->_conn[$this->_group]['master']->watch($key);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('watch key:{' . $key . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
	}

	/**
	 * 取消事务
	 * 
	 * 刷新一个事务中已被监视的所有key。
	 * 如果执行EXEC 或者DISCARD， 则不需要手动执行UNWATCH 。
	 *
	 * @example
	 * $redis->watch('test_key_106');
	 * debug($redis->hIncrBy('test_key_106', 'rank', 2), 0);
	 * $redis->unWatch('test_key_106');		//unlock key
	 * $res = $redis->multi('test_key_106')->hgetall('test_key_106')->hIncrBy('test_key_106', 'rank', 1)->exec();  //return array
	 * 
	 * @param  string  $key
	 * @return void
	 */
	public function unWatch($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$this->_conn[$this->_group]['master']->unWatch();
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('unWatch key:{' . $key . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
	}

	/****** about connection *******/

	public function pingMaster($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->ping();
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('ping master server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	public function pingSlave($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->ping();
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('ping slave server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	public function quitMaster($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->close();
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('quit master server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	public function quitSlave($key) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->close();
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('quit slave server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	public function selectDbByMaster($key, $dbNumber = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_write($key);
		$res = $this->_conn[$this->_group]['master']->select($dbNumber);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('select master dbNumber:{' . $dbNumber . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}

	public function selectDbBySlave($key, $dbNumber = 0) {
		$startTime = RpcLog::getMicroTime();
		$this->_read($key);
		$res = $this->_conn[$this->_group]['slave']->select($dbNumber);
		$endTime = RpcLog::getMicroTime();
		RpcLog::log('select slave dbNumber:{' . $dbNumber . '} server:{host:' . $this->_servers['host'] . ' port:' . $this->_servers['port'] . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_REDIS);
		return $res;
	}
}