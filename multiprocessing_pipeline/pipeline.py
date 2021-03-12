import multiprocessing
import traceback
import os
import os.path as osp

from .subblocks import QueueMsg, Assembler, Processor, Dissembler


class MetaMsg:
    def __init__(self, sender_subblock_name, acceptor_name, acceptor_type, msg):
        assert isinstance(sender_subblock_name, str)
        self.sender_subblock_name = sender_subblock_name

        assert isinstance(acceptor_name, str)
        self.acceptor_name = acceptor_name

        assert isinstance(acceptor_type, str)
        self.acceptor_type = acceptor_type

        assert msg is not None
        self.msg = msg


class MsgProcessor(multiprocessing.Process):
    def __init__(self, msg_queue, assembler_queues, processor_queues, dissembler_queues):
        multiprocessing.Process.__init__(self)
        self.msg_queue = msg_queue
        self.queues = {
            'assembler': assembler_queues,
            'processor': processor_queues,
            'dissembler': dissembler_queues
        }

    def run(self):
        try:
            while True:
                msg = self.msg_queue.get()
                assert isinstance(msg, MetaMsg), str(msg)
                self.queues[msg.acceptor_type][msg.acceptor_name].put(QueueMsg(msg=msg.msg))
        except Exception as e:
            print(f'MsgProcessor Exception')
            print(traceback.format_exc())
        except KeyboardInterrupt as e:
            print(f'MsgProcessor KeyboardInterrupt')
        

class Block:
    '''
    every block cound contain assembler, processor and dissembler subblocks
    '''
    def __init__(self, name, use_assembler=True, use_dissembler=True, skip_assembler=False):
        self.name = name
        self.use_assembler = use_assembler
        self.use_dissembler = use_dissembler
        self.skip_assembler = skip_assembler


class Pipeline:
    def __init__(self, check_cycles=True):
        self.check_cycles = check_cycles

        self.blocks = dict()
        self.outputs = dict()

        self.assembler_queues = dict()
        self.processor_queues = dict()
        self.dissembler_queues = dict()
        
        self.assemblers = dict()
        self.processors = dict()
        self.dissemblers = dict()

        # is the only queue to transmit MetaMsg messages
        # is not suited for QueueEl, QueueMsg or QueueData messages that are used to communicate beetween subblocks
        self.msg_queue = multiprocessing.Queue()  

    def add_block(self, block):
        self.blocks[block.name] = block

    def set_outputs(self, name, output_names):
        assert name in self.blocks, name
        self.outputs[name] = output_names

    def _check_connections(self, name):
        assert self.visited[name] != 1, 'cycle in connections'
        if self.visited[name] == 0:
            self.visited[name] = 1
            if name in self.outputs:
                for k in self.outputs[name]:
                    assert self.blocks[k].use_assembler, 'cannot send output to input block'
                    self._check_connections(k)
            self.visited[name] = 2

    def check_connections(self):
        self.visited = dict()
        for k in self.blocks.keys():
            self.visited[k] = 0
            if (k not in self.outputs or len(self.outputs[k]) == 0) and self.blocks[k].use_dissembler:
                raise Exception(f'dissembler cannot be used for a block {k} with no outputs')
        if self.check_cycles:
            for k in self.blocks.keys():
                if self.visited[k] == 0:
                    self._check_connections(k)

    def create_queues(self):
        for k, v in self.blocks.items():
            self.assembler_queues[k] = multiprocessing.Queue() if v.use_assembler else None
            self.processor_queues[k] = multiprocessing.Queue()
            self.dissembler_queues[k] = multiprocessing.Queue() if v.use_dissembler and len(self.outputs[k]) > 0 else None
        self.msg_processor = MsgProcessor(self.msg_queue, self.assembler_queues, self.processor_queues, self.dissembler_queues)

    def set_assembler(self, name, process_class, **kwargs):
        assert issubclass(process_class, Assembler), name
        assert self.blocks[name].use_assembler, name
        assert not self.blocks[name].skip_assembler
        class_member = process_class(
            name, 
            self.msg_queue,
            self.assembler_queues[name], 
            self.processor_queues[name], 
            **kwargs
        )
        self.assemblers[name] = class_member

    def set_processor(self, name, process_class, **kwargs):
        if self.blocks[name].use_dissembler:
            output_queue = self.dissembler_queues[name]
        elif name not in self.outputs or len(self.outputs[name]) == 0:
            output_queue = None
        elif len(self.outputs[name]) == 1:
            child_name = self.outputs[name][0]
            if self.blocks[child_name].skip_assembler:
                output_queue = self.processor_queues[child_name]
            else:
                output_queue = self.assembler_queues[child_name]
        else:
            raise Exception(f'cannot set processor output queue for block {name}')

        if self.blocks[name].use_assembler or self.blocks[name].skip_assembler:
            input_queue = self.processor_queues[name]
        else:
            input_queue = None

        class_member = process_class(
            name,
            self.msg_queue,
            input_queue=input_queue,
            output_queue=output_queue,
            assembler_input_queue=self.assembler_queues[name] if self.blocks[name].use_assembler else None,
            **kwargs
        )
        self.processors[name] = class_member

    def set_dissembler(self, name, process_class, **kwargs):
        assert issubclass(process_class, Dissembler), name
        assert self.blocks[name].use_dissembler, name
        assert len(self.outputs[name]) > 0, name
        class_member = process_class(
            name, 
            self.msg_queue,
            self.dissembler_queues[name],
            [self.assembler_queues[k] for k in self.outputs[name]],
            **kwargs
        )
        class_member.outputs = self.outputs[name]
        self.dissemblers[name] = class_member
    
    def set_loggers(self, log_dirpath):
        if log_dirpath is not None:
            os.makedirs(log_dirpath, exist_ok=True)
            mapping = [
                ('assembler', self.assemblers),
                ('processor', self.processors),
                ('dissembler', self.dissemblers)
            ]
            for i, block_name in enumerate(self.blocks):
                for j, (subblock_type, subblocks) in enumerate(mapping):
                    if block_name in subblocks:
                        subblocks[block_name].set_logger_fp(
                            osp.join(
                                log_dirpath, 
                                f'{i:02d}_{j:02d} {block_name} {subblock_type}.log'
                            )
                        )

    def start(self, log_dirpath=None):
        self.set_loggers(log_dirpath)
        for d in [self.assemblers, self.processors, self.dissemblers]:
            for v in d.values():
                v.start()
        self.msg_processor.start()
