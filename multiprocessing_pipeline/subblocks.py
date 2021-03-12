import logging
import multiprocessing
import traceback
from copy import deepcopy


PROCESSOR_FED = 'processor_fed'


class QueueEl:
    def __init__(self):
        pass


class QueueData(QueueEl):
    def __init__(self, name, index, value):
        QueueEl.__init__(self)

        assert isinstance(name, str)
        self.name = name

        assert isinstance(index, int), self.name
        self.index = index

        assert value is not None, self.name
        self.value = value

    def __str__(self):
        return f'name: {self.name}, index: {self.index}, value: {self.value}'


class QueueMsg(QueueEl):
    def __init__(self, msg):
        QueueEl.__init__(self)

        assert msg is not None
        self.msg = msg

    def __str__(self):
        return f'msg: {self.msg}'


class SubBlock(multiprocessing.Process):
    def __init__(self, name, msg_queue, input_queue=None, output_queue=None):
        multiprocessing.Process.__init__(self)
        self.name = name
        self.subblock_name = f'{name}_{self.__class__.__name__}'
        self.msg_queue = msg_queue
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.logger = None
        self.logger_fp = None

    def set_logger(self):
        if self.logger_fp is not None:
            self.logger = logging.getLogger()
            self.logger.setLevel(logging.INFO)
            fh = logging.FileHandler(self.logger_fp, mode='w')
            fh.setFormatter(logging.Formatter(
                '%(asctime)s.%(msecs)03d -- %(levelname)s -- %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            self.logger.addHandler(fh)
            self.logger.info('set_logger')

    def set_logger_fp(self, fp):
        self.logger_fp = fp

    def get_log_msg(self, data):
        if data is None:
            return str(None)
        elif isinstance(data, QueueEl):
            if isinstance(data, QueueMsg):
                return f'QueueMsg.msg={data.msg}'
            elif isinstance(data, QueueData):
                return f'QueueData.index={data.index}'
            else:
                raise Exception(f'contact developer, no code for type {type(data)}')
        elif isinstance(data, list):
            result = []
            for el in data:
                result.append(self.get_log_msg(el))
            result = ','.join(result)
            result = f'[{result}]'
            return result
        else:
            raise Exception(f'data type {type(data)} not recognized')

    def log(self, msg, data):
        if self.logger is not None:
            self.logger.info(f'{msg} {self.get_log_msg(data)}')

    def run(self):
        try:
            self.set_logger()
            self.custom_run()
        except KeyboardInterrupt as e:
            print(f'{self.subblock_name} KeyboardInterrupt')
            self.destructor()
        except:
            print(f'{self.subblock_name} Exception')
            print(traceback.format_exc())
            self.destructor()
        
    def custom_run(self, **kwargs):
        raise NotImplementedError

    def destructor(self):
        pass


class Assembler(SubBlock):
    def __init__(self, name, msg_queue, input_queue, output_queue):
        SubBlock.__init__(self, name, msg_queue, input_queue=input_queue, output_queue=output_queue)
        self.hungry_count = 1

    def custom_run(self):
        while True:
            input_queue_els = []
            while True:
                self.log('queue_wait', None)
                input_queue_el = self.input_queue.get()
                self.log('input_queue.get', input_queue_el)
                if input_queue_el is None:
                    break
                assert isinstance(input_queue_el, QueueEl), self.subblock_name
                if isinstance(input_queue_el, QueueMsg) and isinstance(input_queue_el.msg, str) and input_queue_el.msg == PROCESSOR_FED:
                    self.hungry_count += 1
                    continue
                elif isinstance(input_queue_el, QueueMsg):
                    self.process_queue_els([input_queue_el])
                elif isinstance(input_queue_el, QueueData):
                    input_queue_els.append(input_queue_el)
                    if self.hungry_count > 0:
                        break
                else:
                    raise Exception(f'contact developer, no code for {type(input_queue_el)} in subblock {self.subblock_name}')
            output_queue_els = self.process_queue_els(input_queue_els)
            if len(output_queue_els) > 0:
                self.log('output_queue.put', output_queue_els[-1])
            for output_queue_el in output_queue_els:
                if output_queue_el is not None:
                    assert isinstance(output_queue_el, QueueData)
                    self.output_queue.put(output_queue_el)
                    # self.hungry_count = max(self.hungry_count - 1, 0)
                    self.hungry_count = 0
            self.log('tmp', None)

    def process_queue_els(self, **kwargs):
        raise NotImplementedError


class Processor(SubBlock):
    def __init__(
        self,
        name,
        msg_queue,
        input_queue, output_queue, assembler_input_queue,
        deepcopy=False
    ):
        SubBlock.__init__(self, name, msg_queue, input_queue=input_queue, output_queue=output_queue)
        self.assembler_input_queue = assembler_input_queue
        self.deepcopy = deepcopy

    def custom_run(self):
        while True:
            if self.input_queue is not None:
                self.log('queue_wait', None)
                queue_el = self.input_queue.get()
                if self.assembler_input_queue is not None:
                    self.assembler_input_queue.put(QueueMsg(msg=PROCESSOR_FED))
                self.log('input_queue.get', queue_el)
                if queue_el is None:
                    break
                assert isinstance(queue_el, QueueEl), f'input queue el not recognized in subblock {self.subblock_name}'
                if isinstance(queue_el, QueueData):
                    index = queue_el.index
                    value = self.process_value(queue_el.value)
                    if value is None:
                        if self.output_queue is None:
                            self.log('output_queue.put', queue_el)
                        continue
                elif isinstance(queue_el, QueueMsg):
                    self.process_value(queue_el)
                    continue
                else:
                    raise Exception(f'contact developer, no code for {type(queue_el)} in subblock {self.subblock_name}')
            else:
                process_result = self.process_value()
                if process_result is None:
                    continue
                index, value = process_result
            if self.output_queue is not None:
                queue_el = QueueData(name=self.name, index=index, value=value)
                self.log('output_queue.put', queue_el)
                if self.deepcopy:
                    queue_el = deepcopy(queue_el)
                self.output_queue.put(queue_el)
            self.log('tmp', None)
    
    def process_value(self, **kwargs):
        raise NotImplementedError


class Dissembler(SubBlock):
    def __init__(self, name, msg_queue, input_queue, output_queues):
        SubBlock.__init__(self, name, msg_queue, input_queue=input_queue)
        self.output_queues = output_queues

    def custom_run(self):
        while True:
            self.log('queue_wait', None)
            queue_el = self.input_queue.get()
            self.log('input_queue.get', queue_el)
            if queue_el is None:
                break
            assert isinstance(queue_el, QueueEl), f'input queue el not recognized in subblock {self.subblock_name}'
            if isinstance(queue_el, QueueData):
                output_queue_els = self.process_queue_el(queue_el)
                self.log('output_queue.put', queue_el)
                assert len(output_queue_els) == len(self.output_queues)
                for output_queue, output_queue_el in zip(self.output_queues, output_queue_els):
                    if output_queue_el is not None:
                        output_queue.put(output_queue_el)    
                self.log('tmp', None)
            elif isinstance(queue_el, QueueMsg):
                self.process_queue_el(queue_el)
                continue
            else:
                raise Exception(f'contact developer, no code for {type(queue_el)} in subblock {self.subblock_name}')

    def process_queue_el(self, **kwargs):
        raise NotImplementedError


class SkipAssembler(Assembler):
    def __init__(self, name, msg_queue, input_queue, output_queue):
        Assembler.__init__(self, name, msg_queue, input_queue, output_queue)
        self.max_index = 0
        
    def process_queue_els(self, els):
        assert self.process_value is not None, str(self.__class__.__name__)
        if len(els) == 0:
            return []
        max_index_index = -1
        for i in range(len(els)):
            if isinstance(els[i], QueueData):
                if els[i].index > self.max_index:
                    self.max_index = els[i].index
                    max_index_index = i
            elif isinstance(els[i], QueueMsg):
                self.process_value(els[i])
            else:
                raise Exception(f'contact developer, no code for {type(els[i])} in subblock {self.subblock_name}')
        if max_index_index >= 0:
            value = deepcopy(self.process_value(els[max_index_index].value))
            return [QueueData(name=self.name, index=self.max_index, value=value)]
        else:
            return []

    def process_value(self, **kwargs):
        raise NotImplementedError


class NoSkipAssembler(Assembler):
    def __init__(self, name, msg_queue, input_queue, output_queue):
        Assembler.__init__(self, name, msg_queue, input_queue, output_queue)

    def process_queue_els(self, els):
        if len(els) == 0:
            return []
        result = []
        for i in range(len(els)):
            el = els[i]
            if isinstance(el, QueueData):
                result.append(QueueData(name=self.name, index=el.index, value=el.value))
            elif isinstance(el, QueueMsg):
                pass
            else:
                raise Exception(f'contact developer, no code for {type(el)} in subblock {self.subblock_name}')
        return result


class DummySkipAssembler(SkipAssembler):
    def __init__(self, name, msg_queue, input_queue, output_queue):
        SkipAssembler.__init__(self, name, msg_queue, input_queue, output_queue)
    
    def process_value(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            return x


class DummyMultipleSkipAssembler(Assembler):
    def __init__(self, name, msg_queue, input_queue, output_queue, input_names):
        Assembler.__init__(self, name, msg_queue, input_queue, output_queue)
        self.inputs = dict()        
        self.input_names = input_names
        self.max_index = 0
        
    def process_queue_els(self, els):
        if len(els) == 0:
            return []
        for el in els:
            if isinstance(el, QueueData):
                if el.name in self.input_names:
                    index = el.index
                    if not index in self.inputs:
                        self.inputs[index] = dict()
                    self.inputs[index][el.name] = el.value
                else:
                    raise Exception(f'subblock: {self.subblock_name}, el: {str(el)}')
        result = []
        indexes = list(sorted(filter(lambda k: len(self.inputs[k]) == len(self.input_names), self.inputs.keys())))
        if len(indexes) > 0:
            max_index = max(indexes)
            self.max_index = max(self.max_index, max_index)
            if max_index == self.max_index:
                result_value = deepcopy(self.inputs[max_index])
                result_queue_el = QueueData(name=self.name, index=max_index, value=result_value)
                result.append(result_queue_el)
            delete_indexes = list(filter(lambda x: x <= self.max_index, self.inputs.keys()))
            for delete_index in delete_indexes:
                del self.inputs[delete_index]
        return result


class DummyProcessor(Processor):
    def __init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue):
        Processor.__init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue)

    def process_value(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            return x


class DummyDissembler(Dissembler):
    def __init__(self, name, msg_queue, input_queue, output_queues):
        Dissembler.__init__(self, name, msg_queue, input_queue, output_queues)

    def process_queue_el(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            return [deepcopy(x) for _ in range(len(self.output_queues))]
