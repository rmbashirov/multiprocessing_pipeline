import argparse


from multiprocessing_pipeline import Block, Pipeline
from multiprocessing_pipeline import DummySkipAssembler, DummyDissembler
from demo_subblocks import FitShapeAssembler, FitPoseAssembler
from demo_subblocks import K4AProcessor, VINOProcessor, FitShapeProcessor, FitPoseProcessor, ResultProcessor
from demo_subblocks import DummyDisableDissembler


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--use_kinect', action='store_true')
    args = parser.parse_args()
    return args

def main():
    args = parse_args()

    p = Pipeline()

    p.add_block(Block('k4a', use_assembler=False))
    p.add_block(Block('vino'))  # detects face kps
    p.add_block(Block('fit_shape'))  # collects n frames and performs shape fit. after fit is performed sends msg to k4a and vino to stop sending data to fit_shape.
    p.add_block(Block('fit_pose', use_dissembler=False))  # starts to work only after fit_shape is done
    p.add_block(Block('result', use_dissembler=False))  # prints results

    p.set_outputs('k4a', ['vino', 'fit_shape', 'fit_pose'])
    p.set_outputs('vino', ['fit_shape', 'fit_pose'])
    p.set_outputs('fit_shape', ['fit_pose'])
    p.set_outputs('fit_pose', ['result'])

    p.check_connections()
    p.create_queues()

    scale = 10
    
    p.set_processor('k4a', K4AProcessor, sleep_args=(scale * 27, scale * 5), use_kinect=args.use_kinect)
    p.set_dissembler('k4a', DummyDisableDissembler)

    p.set_assembler('vino', DummySkipAssembler)
    p.set_processor('vino', VINOProcessor, sleep_args=(scale * 80, scale * 20))
    p.set_dissembler('vino', DummyDisableDissembler)

    p.set_assembler('fit_shape', FitShapeAssembler, n_frames=10)
    p.set_processor('fit_shape', FitShapeProcessor, sleep_args=(scale * 1000, scale * 100))
    p.set_dissembler('fit_shape', DummyDissembler)

    p.set_assembler('fit_pose', FitPoseAssembler)
    p.set_processor('fit_pose', FitPoseProcessor, sleep_args=(scale * 200, scale * 40))

    p.set_assembler('result', DummySkipAssembler)
    p.set_processor('result', ResultProcessor)

    p.start()


if __name__ == '__main__':
    main()
