<?php namespace PHPBook\ETL;

class Routine {

    private $name;

    private $priority;

    public static $PRIORITY_EXTERNAL = 'PRIORITY_EXTERNAL';

    public static $PRIORITY_LOCAL = 'PRIORITY_LOCAL';

    public function __construct($name) {

        $this->name = $name;

        $this->priority = self::$PRIORITY_EXTERNAL;

    }

    public function priority($priority) {

        $this->priority = $priority;

    }

    public function run() {

        try {

            $setup = \PHPBook\ETL\Configuration\Setup::get($this->name);

            if ($setup) {

                foreach($setup->schemas as $schema) {

                    $classSchema = $schema->mapper;

                    $schema = new $classSchema($setup);

                    $schema->run($this->priority);

                };

            };       

        } catch (\Exception $e) {

            if (\PHPBook\ETL\Configuration\Setup::getExceptionCatcher()) {

                \PHPBook\ETL\Configuration\Setup::getExceptionCatcher()($e->getMessage());

            }

        }

    }

}