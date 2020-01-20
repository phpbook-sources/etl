<?php namespace PHPBook\ETL\Configuration;

abstract class Setup {

	private static $setups = [];

	private static $exceptionCatcher = null;

	public static function setSetup(\PHPBook\ETL\Setup $setups) {

		self::$exceptionCatcher = $setups->getExceptionCatcher();

		try {

			if (count(self::$setups) > 0) {

				throw new \Exception("ETL configuration already exists");

			} else {

				foreach($setups->getFiles() as $file) {

					$setup = json_decode(file_get_contents($file));

					if ($setup) {

						if (!array_key_exists($setup->name, self::$setups)) {
				
							self::$setups[$setup->name] = $setup; 

						} else {

							throw new \Exception("ETL configuration setup name " . $setup->name . ' in use');

						}

					} else {

						throw new \Exception("ETL configuration setup file " . $location . ' is not valid');
						
					}

				}

			}

		} catch (\Exception $e) {

			if (self::$exceptionCatcher) {

				self::$exceptionCatcher($e->getMessage());

			}

		}

	}

	public static function getExceptionCatcher(): ?\Closure {

		return self::$exceptionCatcher;

	}

	public static function get($name) {

		return array_key_exists($name, self::$setups) ? self::$setups[$name] : null;

	}


}