<?php

namespace Qless;

/**
 * The base class for all qless exceptions
 */
class QlessException extends \Exception
{
    /**
     * @param string      $message
     * @param string|null $area
     * @param int         $code
     */
    public function __construct($message, private $area = null, $code = 0, ?\Exception $previous = null)
    {
        parent::__construct($message, $code, $previous);
    }

    /**
     * @param string $error
     */
    public static function createExceptionFromError($error): QlessException
    {
        if (preg_match('/^ERR.*user_script:\d+:\s*(?<area>[\w.]+)\(\):\s*(?<message>.*)/', $error, $matches) > 0) {
            $area    = $matches['area'];
            $message = $matches['message'];
        } else {
            $area    = null;
            $message = $error;
        }

        return match (true) {
            stripos($message, 'does not exist') !== false              => new InvalidJobException($message, $area),
            stripos($message, 'given out to another worker') !== false => new JobLostException($message, $area),
            default                                                    => new QlessException($message, $area),
        };
    }

    /**
     * @return string|null
     */
    public function getArea()
    {
        return $this->area;
    }
}
