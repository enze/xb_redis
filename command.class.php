<?php

class Command {

	static public function getMasters() {
		return array('SENTINEL', 'masters');
	}

	static public function getSlavesByMaster($masterName) {
		return array('SENTINEL', 'slaves', $masterName);
	}

	static public function getMasterAddrByName($masterName) {
		return array('SENTINEL', 'get-master-addr-by-name', $masterName);
	}
}