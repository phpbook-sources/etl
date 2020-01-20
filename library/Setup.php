<?php namespace PHPBook\ETL;

class Setup {

	private $files;

	private $exceptionCatcher;

	public function getFiles(): array {
		return $this->files;
	}

	public function setFiles(array $files): Setup {
		$this->files = $files;
		return $this;
	}

	public function getExceptionCatcher(): ?\Closure {
		return $this->exceptionCatcher;
	}

	public function setExceptionCatcher(\Closure $exceptionCatcher): Setup {
		$this->exceptionCatcher = $exceptionCatcher;
		return $this;
	}

}
