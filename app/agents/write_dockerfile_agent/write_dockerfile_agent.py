from app.data_structures import MessageThread
from app.agents.write_dockerfile_agent import write_dockerfile_utils
from app.agents.agent import Agent
from app.task import Task
import os
import shutil
from loguru import logger
import re
import json
from app.log import (
    print_acr,
    print_banner,
    print_retrieval,
)
from os.path import join as pjoin


class WriteDockerfileAgent(Agent):
    """
    LLM-based agent for creating or modifying a Dockerfile via direct chat.
    Manages its own create/modify logic, output directories, and retry behavior.
    """
    api_functions: list[str] = []
    def __init__(self,  task: Task, output_dir: str, repo_basic_info: str, using_ubuntu_only: bool = False):
        super().__init__(agent_id="WriteDockerfileAgent")
        self.msg_thread  = MessageThread()
        self.task = task
        self.output_dir = os.path.abspath(output_dir)
        self.run_count = 0
        self.reference_setup = None
        self.repo_basic_info = repo_basic_info
        self.using_ubuntu_only = using_ubuntu_only
        self.dockerfile_hint = self._load_dockerfile_hint_from_results()
        self.init_msg_thread()


    def init_msg_thread(self) -> None:
        self.msg_thread = MessageThread()
        self.add_system_message(write_dockerfile_utils.get_system_prompt_dockerfile())
        self.add_user_message(self.repo_basic_info)
        # Add dockerfile hint if available
        if self.dockerfile_hint:
            self.add_user_message(self.dockerfile_hint)

    def _load_dockerfile_hint_from_results(self) -> str:
        """
        Load results.json and find matching repo to extract dockerfile hint.
        Returns formatted hint message or empty string if not found.
        """
        try:
            # Get repo_name from task (only available for SweTask)
            task_repo = getattr(self.task, 'repo_name', None)
            if not task_repo:
                logger.debug("Task does not have repo_name attribute")
                return ""
            
            # Construct path to results.json
            # output_dir is like: swe-factory/output_full_one/applicable_setup/beanstalkd__beanstalkd-151
            # We need to go up to output_full_one and then to results/results.json
            base_output_dir = self.output_dir
            # Navigate up to find the base output directory (e.g., output_full_one)
            while base_output_dir and not base_output_dir.endswith(('output_full_one', 'output_full_oue')):
                parent = os.path.dirname(base_output_dir)
                if parent == base_output_dir:  # reached root
                    break
                base_output_dir = parent
            
            results_path = os.path.join(base_output_dir, 'results', 'results.json')
            
            if not os.path.exists(results_path):
                logger.debug(f"Results file not found at {results_path}")
                return ""
            
            with open(results_path, 'r') as f:
                results = json.load(f)
            
            # Find matching repo
            for item in results:
                if item.get('repo') == task_repo:
                    dockerfile = item.get('dockerfile', '')
                    if dockerfile:
                        hint_message = (
                            f"HINT: A successful Dockerfile was previously created for this repository ({task_repo}). "
                            "You can use it as a reference to understand the setup requirements:\n\n"
                            f"```dockerfile\n{dockerfile}\n```\n\n"
                            "Feel free to adapt or use this as inspiration for the current task."
                        )
                        logger.info(f"Loaded dockerfile hint for repo: {task_repo}")
                        return hint_message
            
            logger.debug(f"No matching repo found in results.json for: {task_repo}")
            return ""
            
        except Exception as e:
            logger.warning(f"Failed to load dockerfile hint from results.json: {e}")
            return ""

    def add_reference_message(self) -> None:
        if self.reference_setup:
            reference_version = self.reference_setup['version']
            reference_dockerfile =self.reference_setup['dockerfile']
            reference_text = (
                f"I found a Dockerfile from version {reference_version} of this repo that worked well in a similar setup. "
                "You might consider it as a reference—if its configuration aligns with your current environment, it could "
                "save you some effort. Otherwise, feel free to adapt or disregard as needed:\n\n"
                f"{reference_dockerfile}"
            )
            self.add_user_message(reference_text)


    def run_task(self, print_callback=None) -> tuple[str, str, bool]:
        """
        Create or modify a Dockerfile based on the given message_thread context.
        Handles versioning, directory management, and fallback copy logic.
        """
        # 1. Determine previous vs current output paths
        print_banner(f"Iteration ROUND {self.iteration_num}: Dockerfile Generation ")
        prev_dir = self.get_latest_write_dockerfile_output_dir()
        prev_file = os.path.join(prev_dir, 'Dockerfile')
        self.run_count += 1
        curr_dir = self.get_latest_write_dockerfile_output_dir()
        os.makedirs(curr_dir, exist_ok=True)
        self.add_reference_message()
        # 2. Inject either modify or init prompt
        if os.path.exists(prev_file):
            modify_prompt = write_dockerfile_utils.get_user_prompt_modify_dockerfile()
            # add previous Dockerfile content
            prev_content = self._read_file(prev_file)
            self.add_user_message(f"Previous dockerfile:\n{prev_content}\n")
            self.add_user_message(modify_prompt)
        else:
            if self.using_ubuntu_only:
                self.add_user_message(write_dockerfile_utils.get_user_prompt_init_dockerfile_using_ubuntu_only())
            else:
                self.add_user_message(write_dockerfile_utils.get_user_prompt_init_dockerfile())

        # 3. Delegate to the retryable writer
        task_output = write_dockerfile_utils.write_dockerfile_with_retries(
            self.msg_thread,
            curr_dir,
            self.task,
            print_callback=print_callback
        )

        # 4. Post-process: validate or fallback copy
        dockerfile_path = os.path.join(curr_dir, 'Dockerfile')
        if not os.path.isfile(dockerfile_path):
            
            # fallback: copy previous
            if os.path.exists(prev_file):
                shutil.copy(prev_file, dockerfile_path)
            summary = "Dockerfile generation failed."
            is_ok = False
        else:
            summary = "Dockerfile created/updated successfully." 
            is_ok = True

        dockerfile_output_dir = self.get_latest_write_dockerfile_output_dir()
        conversation_file = pjoin(dockerfile_output_dir, f"conversation.json")
        self.msg_thread.save_to_file(conversation_file)
        # self.init_msg_thread()
        return task_output, summary, is_ok

    def _read_file(self, path: str) -> str:
        try:
            with open(path, 'r') as f:
                return f.read()
        except Exception:
            return ""

    def get_latest_write_dockerfile_output_dir(self) -> str:
        """
        Return the directory of the most recent Dockerfile outputs.
        """
        return os.path.join(self.output_dir, f"write_dockerfile_agent_{self.run_count}")

    def get_latest_dockerfile(self) -> str:
        """
        Read and return contents of the latest generated Dockerfile.
        """
        path = os.path.join(self.get_latest_write_dockerfile_output_dir(), 'Dockerfile')
        try:
            with open(path, 'r') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Failed to read latest Dockerfile at {path}: {e}")
            return ""
