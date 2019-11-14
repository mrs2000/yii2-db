<?php

namespace mrssoft\db;

use Yii;
use yii\base\ErrorException;
use yii\db\Connection;
use yii\helpers\ArrayHelper;

/**
 * Пакетная вставка / обновление записей
 *
 * @property int $count
 * @property array $data
 * @property array $keys
 */
class DbBatch extends \yii\base\Component
{
    public const COMMAND_INSERT = 'insert';
    public const COMMAND_REPLACE = 'replace';

    private $data = [];

    public $autoFreeMemory = true;

    /**
     * @var \yii\db\Connection
     */
    public $db;

    /**
     * Максимальное кол-во записей перед вставкой
     * @var int
     */
    public $maxItemsInQuery;

    /**
     * Таблица БД {{%table}}
     * @var string
     */
    public $table;

    /**
     * Комманда INSERT или REPLACE
     * @var string
     */
    public $command = self::COMMAND_INSERT;

    /**
     * Предварительная очистка таблицы
     * @var bool
     */
    public $truncate = false;

    private $isTruncate = false;

    public function init()
    {
        if ($this->db === null) {
            $this->db = self::getDatabase();
        }

        parent::init();
    }

    /**
     * @return \yii\db\Connection
     */
    private static function getDatabase(): Connection
    {
        return Yii::$app->db;
    }

    /**
     * Добавить запись
     * @param array $data
     * @param mixed $key
     * @throws ErrorException
     */
    public function add(array $data, ?string $key = null): void
    {
        if ($key === null) {
            $this->data[] = $data;
        } else {
            $this->data[$key] = $data;
        }

        if ($this->table !== null && $this->maxItemsInQuery !== null && count($this->data) >= $this->maxItemsInQuery) {
            $this->execute();
        }
    }

    /**
     * Добавить значение с проверкой на уникальность
     * @param array $data
     * @throws ErrorException
     */
    public function addUnique(array $data): void
    {
        $this->add($data, implode('-', $data));
    }

    /**
     * Удаление записи
     * @param string $key
     */
    public function remove(string $key): void
    {
        if (array_key_exists($key, $this->data)) {
            unset($this->data[$key]);
        }
    }

    /**
     * Проверка наличия записи
     * @param string $key
     * @return bool
     */
    public function exists(string $key): bool
    {
        return array_key_exists($key, $this->data);
    }

    /**
     * Кол-во элементов
     * @return int
     */
    public function getCount(): int
    {
        return count($this->data);
    }

    /**
     * Плучение значения элемента
     * @param string $key
     * @return mixed|null
     */
    public function get(string $key)
    {
        if (array_key_exists($key, $this->data)) {
            return $this->data[$key];
        }

        return null;
    }

    /**
     * Получить массив всех данных
     * @return array
     */
    public function getData(): array
    {
        return array_values($this->data);
    }

    /**
     * Массив данных
     * @param array $array
     */
    public function setData(array $array = []): void
    {
        $this->data = $array;
    }

    /**
     * Массив ключей
     * @return array
     */
    public function getKeys(): array
    {
        return array_keys($this->data);
    }

    /**
     * Выполнить вставку
     * @param $table
     * @param bool $truncate
     * @return bool
     */
    public function insert(string $table, bool $truncate = false): bool
    {
        if ($truncate) {
            $this->truncate($table);
        }
        return $this->executeCommand('INSERT', $table);
    }

    /**
     * Выполнить замену
     * @param $table
     * @param bool $truncate
     * @return bool
     */
    public function replace(string $table, bool $truncate = false): bool
    {
        if ($truncate) {
            $this->truncate($table);
        }
        return $this->executeCommand('REPLACE', $table);
    }

    /**
     * Выполнить комманду
     * @throws ErrorException
     */
    public function execute(): void
    {
        if ($this->table === null) {
            throw new ErrorException('Invalid param: table', 500);
        }

        if (in_array($this->command, [self::COMMAND_INSERT, self::COMMAND_REPLACE], true) === false) {
            throw new ErrorException('Invalid param: command', 500);
        }

        $this->{$this->command}($this->table, $this->truncate && !$this->isTruncate);
    }

    /**
     * Обновить запись
     * @param array $data
     * @param mixed $key
     */
    public function update(array $data, string $key): void
    {
        if (array_key_exists($key, $this->data)) {
            $this->data[$key] = ArrayHelper::merge($this->data[$key], $data);
        }
    }

    /**
     * Вкыл. / выкл. проверку внешних ключей
     * @param bool $value
     * @param \yii\db\Connection $db
     * @throws \yii\db\Exception
     */
    public static function setForeignKey(bool $value, $db = null): void
    {
        $val = $value ? '1' : '0';
        $sql = 'SET FOREIGN_KEY_CHECKS = ' . $val;

        if ($db === null) {
            $db = self::getDatabase();
        }

        $db->createCommand($sql)
           ->execute();
    }

    private function truncate(string $table): void
    {
        $this->db->createCommand()
                 ->truncateTable($table)
                 ->execute();
        $this->isTruncate = true;
    }

    /**
     * Выполнить запрос
     * @param $table
     * @param $command
     * @return bool
     */
    private function executeCommand(string $command, string $table): bool
    {
        if (empty($this->data)) {
            return false;
        }

        $this->compile($command, $table);
        if ($this->autoFreeMemory) {
            $this->data = [];
            gc_collect_cycles();
        }

        return true;
    }

    /**
     * Сформировать строку запроса
     * @param $command
     * @param $table
     */
    private function compile(string $command, string $table): void
    {
        $fields = [];
        foreach (reset($this->data) as $row => $tmp) {
            $fields[] = '`' . $row . '`';
        }

        $n = 0;
        $values = [];
        $pdo = $this->db->getSlavePdo();
        $command .= ' INTO ' . $table . ' (' . implode(',', $fields) . ') VALUES ';

        foreach ($this->data as $row) {
            foreach ($row as &$v) {
                $v = $v === null ? 'NULL' : $pdo->quote($v);
            }
            unset($v);
            $values[] = '(' . implode(',', $row) . ')';

            if ($this->maxItemsInQuery !== null && $this->maxItemsInQuery == ++$n) {
                $this->executePartial($command, $values);
                $n = 0;
            }
        }

        if (count($values) > 0) {
            $this->executePartial($command, $values);
        }
    }

    private function executePartial(string &$command, array &$values): void
    {
        $this->db->createCommand($command . implode(',', $values))
                 ->execute();
        $values = [];
        gc_collect_cycles();
    }
}