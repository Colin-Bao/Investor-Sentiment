#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :tools.py
# @Time      :2023/1/4 14:38
# @Author    :Colin
# @Note      :None


class MultiExecutor:
    """
        多进程任务处理
    """

    def __init__(self, **kwargs):
        # 用于下载的线程数量
        self.MAX_CORE = kwargs.get('MAX_CORE', 10)

        # 下载过程信息
        self.Lock = None
        self.Total_Tasks = 0
        self.Completed_Tasks = 0
        self.Pbar = None

    def start_multi_task(self, func, task_list: list):
        """
        多进程处理
        """
        from concurrent.futures import ThreadPoolExecutor
        from threading import Lock
        from tqdm import tqdm

        #
        self.Lock = Lock()
        self.Total_Tasks = len(task_list)
        self.Completed_Tasks = 0
        self.Pbar = tqdm(total=len(task_list))

        # 回调
        def progress_indicator(future_arg):
            with self.Lock:  # obtain the lock
                self.Completed_Tasks += 1
                self.Pbar.update(1)

        # 任务开始
        with ThreadPoolExecutor(max_workers=self.MAX_CORE) as executor:
            futures = [executor.submit(func, task) for task in tqdm(task_list)]
            for future in futures:
                future.add_done_callback(progress_indicator)
