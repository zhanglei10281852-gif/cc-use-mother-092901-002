# 车路云事件可信合成

本项目定义车路云事件观测与发布快照的基础契约。观测保留来源、外部编号和实际观测时刻，便于后续实现冲突判断、撤回、复发识别及历史重演。

运行测试：\`python -m unittest discover -s tests -v\`

编译检查：\`python -m compileall -q src tests run_cli.py\`

命令行冒烟：\`python run_cli.py\`
