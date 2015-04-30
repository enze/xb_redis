<?php

class RedisStream {

	static private $_stream = null;
	private $_socket = null;

	private $_retry = 0;

	private $_errno = 0;
	private $_errstr = '';

	private function __construct($retry = 0) {
		$this->_retry = (0 === (int) $retry) ? 3 : $retry;
	}

	static public function instance($retry = 0) {
		if (null === self::$_stream) {
			self::$_stream = new self($retry);
		}
		return self::$_stream;
	}

	public function create($host, $port, $protocol, $timeout = 3, $flags = STREAM_CLIENT_CONNECT) {
		$protocol = ('TCP' == strtoupper($protocol) ? 'tcp' : 'udp');
		$times = 1;
		do {
			if ($times > $this->_retry) {
				throw new ResourceException('-9000001', 'create stream socket fail! [errorinfo: (' . $this->_errno . ') ' . $this->_errstr . ']');
				break;
			}
			try {
				$startTime = RpcLog::getMicroTime();
				$this->_socket = stream_socket_client($protocol . '://' . $host . ':' . $port, $this->_errno, $this->_errstr, $timeout, $flags);
				$endTime = RpcLog::getMicroTime();
				if ($this->_socket) {
					RpcLog::log('socket create success server:{protocol:' . $protocol . ' host:' . $host . ' port:' . $port . '}', $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_SOCKET);
					break;
				}
				RpcLog::log('socket create error server:{protocol:' . $protocol . ' host:' . $host . ' port:' . $port . '} (' . $this->_errno . ') ' . iconv('GBK', 'UTF-8', $this->_errstr), $startTime, $endTime, RpcLogEnvConfig::RPC_LOG_TYPE_SOCKET);
			} catch (Exception $e) {
			}
			$times ++;
		} while (true);
		return $this->_socket;
	}

	public function request($args) {
		$command = '*' . count($args) . "\r\n";
		foreach ($args as $arg) {
			$command .= '$' . strlen($arg) . "\r\n" . $arg . "\r\n";
		}
		$times = 1;
		do {
			if ($times > $this->_retry) {
				return false;
				break;
			}
			@ $send = fwrite($this->_socket, $command, strlen($command));
			if ($send) {
				break;
			}
			$times ++;
		} while (true);
	}

	public function response() {
		$times = 1;
		do {
			if ($times > $this->_retry) {
				return false;
				break;
			}
			stream_set_timeout($this->_socket, 3);
			@ $serverReply = fgets($this->_socket);
			if ($serverReply) {
				break;
			}
			$times ++;
		} while (true);
		$serverReply = trim($serverReply);
		$response = '';
		$replyTips = (int)substr($serverReply, 1);
		switch ($serverReply[0]) {
			case '-':
				$this->_outPutMessage($serverReply);
				$response = false;
				break;
			case '+':
				$response = $replyTips;
				break;
			case '*':
				$response = array();
				$total = $replyTips;
				for ($i = 0; $i < $total; $i++) {
					$response[] = $this->response();
				}
				break;
			case '$':
				$total = $replyTips;
				if ('-1' == $total) {
					$response = null;
				} else {
					if ($total > 0) {
						$response = stream_get_contents($this->_socket, $total);
					}
					fread($this->_socket, 2);
				}
				break;
			case ':':
				$response = $replyTips;
				break;
			default:
				$this->_outPutMessage($replyTips);
				$response = false;
				break;
		}
		return $response;
	}

	public function __set($var, $val) {
		$var = '_' . $var;
		if (true === property_exists($this, $var)) {
			$this->$var = $val;
		}
	}

	/**
	 * 错误信息
	 *
	 * @param string $message
	 */
	private function _outPutMessage($message) {
		trigger_error($message);
	}
}