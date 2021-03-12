import numpy as np
from copy import deepcopy
from collections import defaultdict
import time

from multiprocessing_pipeline import Assembler, Processor, Dissembler
from multiprocessing_pipeline import QueueMsg, QueueData, MetaMsg


def get_result(x, name):
    if isinstance(x, str):
        if name is None:
            return str(x)
        else:
            return f'({x} -> {name})'
    elif isinstance(x, dict):
        results = [get_result(v, None) for k, v in x.items()]
        result = '; '.join(results)
        result = '{' + result + '}'
        return get_result(result, name)
    elif isinstance(x, list):
        if len(x) == 1:
            return get_result(x[0], name)
        results = [get_result(v, None) for v in x]
        result = '; '.join(results)
        result = '[' + result + ']'
        return get_result(result, name)
    else:
        raise Exception(f'{type(x)}')


def get_sleep_time(mean_ms, variance_ms, seconds=True):
    result = np.random.uniform(mean_ms - variance_ms, mean_ms + variance_ms, 1)[0]
    if seconds:
        result = result / 1000
    return result


class Sleeper:
    def __init__(self, mean, variance):
        self.mean = mean
        self.variance = variance

    def sleep(self):
        sleep_time = get_sleep_time(self.mean, self.variance)
        time.sleep(sleep_time)


class K4AProcessor(Processor):
    def __init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue, sleep_args, use_kinect):
        Processor.__init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue)
        self.sleeper = Sleeper(*sleep_args)
        self.index = -1
        self.use_kinect = use_kinect
        if self.use_kinect:
            self.k4a = None

    def process_value(self):
        self.index += 1
        if self.use_kinect:
            if self.k4a is None:
                import pyk4a
                from pyk4a import Config, PyK4A, ColorResolution

                k4a = PyK4A(Config(
                    color_resolution=ColorResolution.RES_1536P,
                    depth_mode=pyk4a.DepthMode.NFOV_UNBINNED
                ))
                k4a.connect(lut=True)
                self.k4a = k4a

            result = self.k4a.get_capture2(verbose=30) 
            # can return result here
        else:
            self.sleeper.sleep()
        return self.index, get_result(f'{self.index}', self.name)


class VINOProcessor(Processor):
    def __init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue, sleep_args):
        Processor.__init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue)
        self.sleeper = Sleeper(*sleep_args)
    
    def process_value(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            self.sleeper.sleep()
            return get_result(x, self.name)


class FitShapeAssembler(Assembler):
    def __init__(self, name, msg_queue, input_queue, output_queue, n_frames):
        Assembler.__init__(self, name, msg_queue, input_queue, output_queue)
        self.input_values = defaultdict(dict)
        self.input_names = ['vino', 'k4a']
        self.max_index = 0
        self.n_frames = n_frames
        self.done = False
        
    def process_queue_els(self, els):
        if len(els) == 0 or self.done:
            return []
        for el in els:
            if isinstance(el, QueueData):
                if el.name in self.input_names:
                    index = el.index
                    self.input_values[index][el.name] = el.value
                else:
                    raise Exception(str(el))
        result = []
        indexes = list(sorted(filter(lambda k: len(self.input_values[k]) == len(self.input_names), self.input_values.keys())))
        if len(indexes) >= self.n_frames:
            self.done = True
            print(f'collected data for {self.name}')
            max_index = max(indexes)
            self.max_index = max(self.max_index, max_index)
            if max_index == self.max_index:
                value = [deepcopy(self.input_values[k]) for k in indexes[-self.n_frames:]]
                result.append(QueueData(name=self.name, index=max_index, value=value))
            delete_indexes = list(filter(lambda x: x <= self.max_index, self.input_values.keys()))
            for delete_index in delete_indexes:
                del self.input_values[delete_index]
        return result


class FitShapeProcessor(Processor):
    def __init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue, sleep_args):
        Processor.__init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue)
        self.sleeper = Sleeper(*sleep_args)
    
    def process_value(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            print('performing shape fitting')
            self.sleeper.sleep()
            print('fit shape done')
            self.msg_queue.put(MetaMsg(
                sender_subblock_name=self.subblock_name, 
                acceptor_name='vino',
                acceptor_type='dissembler', 
                msg={'disable': 'fit_shape'}
            ))
            self.msg_queue.put(MetaMsg(
                sender_subblock_name=self.subblock_name, 
                acceptor_name='k4a', 
                acceptor_type='dissembler', 
                msg={'disable': 'fit_shape'}
            ))
            return get_result(x, self.name)


class FitPoseAssembler(Assembler):
    def __init__(self, name, msg_queue, input_queue, output_queue):
        Assembler.__init__(self, name, msg_queue, input_queue, output_queue)
        self.inputs = dict()
        self.input_names = ['k4a', 'vino', 'fit_shape']
        self.shape_fit = False
        self.max_index = 0
        
    def process_queue_els(self, els):
        if len(els) == 0:
            return []
        for el in els:
            if isinstance(el, QueueData):
                if el.name == 'fit_shape':
                    self.shape_fit = True
                elif self.shape_fit:
                    if el.name in self.input_names:
                        index = el.index
                        if not index in self.inputs:
                            self.inputs[index] = dict()
                        self.inputs[index][el.name] = el.value
                    else:
                        raise Exception(f'subblock: {self.subblock_name}, el: {str(el)}')
        result = []
        if not self.shape_fit:
            return result
        indexes = list(sorted(filter(lambda k: len(self.inputs[k]) == len(self.input_names) - 1, self.inputs.keys())))
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


class FitPoseProcessor(Processor):
    def __init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue, sleep_args):
        Processor.__init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue)
        self.sleeper = Sleeper(*sleep_args)
    
    def process_value(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            self.sleeper.sleep()
            return get_result(x, self.name)


class ResultProcessor(Processor):
    def __init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue):
        Processor.__init__(self, name, msg_queue, input_queue, output_queue, assembler_input_queue)
    
    def process_value(self, x):
        if isinstance(x, QueueMsg):
            return None
        else:
            print(f'result: {x}')


class DummyDisableDissembler(Dissembler):
    def __init__(self, name, msg_queue, input_queue, output_queues):
        Dissembler.__init__(self, name, msg_queue, input_queue, output_queues)
        self.enabled = [True for _ in range(len(self.output_queues))]

    def process_queue_el(self, x):
        if isinstance(x, QueueMsg):
            if isinstance(x.msg, dict) and 'disable' in x.msg:
                disable_block_name = x.msg['disable']
                disable_index = self.outputs.index(disable_block_name)
                self.enabled[disable_index] = False
                print(f'subblock {self.subblock_name} stopped sending msgs to {disable_block_name}')
        else:
            result = []
            for q, enabled in zip(self.output_queues, self.enabled):
                result.append(deepcopy(x) if enabled else None)
            return result
