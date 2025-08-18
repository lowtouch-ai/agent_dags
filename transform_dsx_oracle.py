from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import re
import os
import json
import git  # Requires gitpython
from github import Github  # Requires pygithub
from ollama import Client  # Assuming ollama for AI agent
from urllib.parse import urlparse
import tempfile
import shutil

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

default_args = {
    "owner": "your_team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# AI agent configuration
OLLAMA_HOST = Variable.get("OLLAMA_HOST", "http://agentomatic:8000/")
AI_MODEL = "Neteeza2Oracle:0.3"

# Processed files tracking file
PROCESSED_FILES_PATH = "/tmp/processed_dsx_files_sample_4.json"
IN_PROGRESS_FILES_PATH = "/tmp/in_progress_dsx_files_sample_2.json"

def get_section_indentation(section):
    """Extract the indentation pattern from the original section."""
    lines = section.split('\n')
    for line in lines:
        if line.strip() and not line.strip().startswith('BEGIN') and not line.strip().startswith('END'):
            # Find the first non-empty line that's not BEGIN/END
            stripped = line.lstrip()
            if stripped:
                logging.debug(f"Indentation found: '{line[:len(line) - len(stripped)]}'")
                return line[:len(line) - len(stripped)]  # Return the leading whitespace
    return ""  # Default to no indentation

def preserve_indentation(original_section, transformed_section):
    """Preserve the original indentation in the transformed section, preferring line-by-line matching."""
    original_lines = original_section.split('\n')
    transformed_lines = transformed_section.split('\n')
    
    if len(original_lines) == len(transformed_lines):
        preserved_lines = []
        for o_line, t_line in zip(original_lines, transformed_lines):
            if not t_line.strip():
                preserved_lines.append(t_line)
                continue
            # Get original leading whitespace
            leading = o_line[:len(o_line) - len(o_line.lstrip())]
            # Strip transformed line and add original leading
            stripped_t = t_line.lstrip()
            preserved_lines.append(leading + stripped_t)
        return '\n'.join(preserved_lines)
    else:
        logging.warning(f"Line count mismatch in section: original {len(original_lines)}, transformed {len(transformed_lines)}. Falling back to uniform indentation.")
        # Fallback to uniform indentation
        uniform_indent = get_section_indentation(original_section)
        if not uniform_indent:
            return transformed_section
        lines = transformed_section.split('\n')
        preserved_lines = []
        for line in lines:
            if line.strip():
                if line.strip().startswith('BEGIN') or line.strip().startswith('END'):
                    preserved_lines.append(line)
                else:
                    stripped_line = line.lstrip()
                    if stripped_line:
                        preserved_lines.append(uniform_indent + stripped_line)
                    else:
                        preserved_lines.append(line)
            else:
                preserved_lines.append(line)
        return '\n'.join(preserved_lines)



def load_in_progress_files():
    """Load the list of in-progress DSX files."""
    try:
        if os.path.exists(IN_PROGRESS_FILES_PATH):
            with open(IN_PROGRESS_FILES_PATH, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        return set()
    except Exception as e:
        logging.error(f"Error loading in-progress files: {str(e)}")
        return set()

def save_in_progress_files(in_progress_files):
    """Save the list of in-progress DSX files."""
    try:
        with open(IN_PROGRESS_FILES_PATH, 'w', encoding='utf-8') as f:
            json.dump(list(in_progress_files), f)
    except Exception as e:
        logging.error(f"Error saving in-progress files: {str(e)}")
        raise

def get_ai_response(prompt):
    try:
        logging.info(f"Calling AI agent with prompt: {prompt}...")
        client = Client(host=OLLAMA_HOST, headers={'x-ltai-client': 'Neteeza2Oracle'})
        response = client.chat(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            stream=False
        )
        ai_content = response.get('message', {}).get('content', "").strip()
        logging.info(f"AI agent response: {ai_content[:200]}...")
        if not ai_content:
            raise ValueError("Empty response from AI agent")
        return ai_content
    except Exception as e:
        logging.error(f"Error calling AI agent: {str(e)}")
        raise

def load_processed_files():
    """Load the list of processed DSX files."""
    try:
        if os.path.exists(PROCESSED_FILES_PATH):
            with open(PROCESSED_FILES_PATH, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        return set()
    except Exception as e:
        logging.error(f"Error loading processed files: {str(e)}")
        return set()

def save_processed_files(processed_files):
    """Save the list of processed DSX files."""
    try:
        with open(PROCESSED_FILES_PATH, 'w', encoding='utf-8') as f:
            json.dump(list(processed_files), f)
    except Exception as e:
        logging.error(f"Error saving processed files: {str(e)}")
        raise

def transform_dsx_file(ti, **context):
    try:
        dsx_filename = context['dag_run'].conf.get("dsx_filename")
        if not dsx_filename:
            raise ValueError("No dsx_filename provided in conf")
        
        # Retrieve GitHub token and base URL
        gh_token = Variable.get("GITHUB_TOKEN")
        base_git_url = Variable.get("DSX_GIT_URL", "https://github.com/hemanth-ak7/datastage-jobs.git")
        
        # Remove '.git' from the URL if present and construct authenticated URL
        base_git_url = base_git_url.rstrip('.git')
        git_url = f"https://{gh_token}@github.com{base_git_url.split('github.com')[1]}.git"
        poc_dir = "jobs/POC"

        parsed = urlparse(base_git_url)
        repo_name = parsed.path.strip('/').replace('.git', '')
        
        # Clone to temp dir
        repo_path = tempfile.mkdtemp(prefix="dsx_transform_")
        ti.xcom_push(key="repo_path", value=repo_path)
        try:
            repo = git.Repo.clone_from(git_url, repo_path)
            
            dsx_path = os.path.join(repo_path, poc_dir, dsx_filename)
            
            if not os.path.exists(dsx_path):
                raise FileNotFoundError(f"DSX file not found: {dsx_path}")
            
            # Read DSX content
            with open(dsx_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            
            # Split DSX content
            header_match = re.search(r'BEGIN HEADER.*?END HEADER', content, re.DOTALL | re.MULTILINE)
            dsx_header = header_match.group(0) + "\n" if header_match else ""
            
            dsjob_match = re.search(r'BEGIN DSJOB.*?END DSJOB', content, re.DOTALL | re.MULTILINE)
            if not dsjob_match:
                logging.warning(f"No DSJOB section found in {dsx_filename}, skipping")
                return
            dsjob_content = dsjob_match.group(0)
            
            job_begin_match = re.match(r'BEGIN DSJOB\s*(.*?)\s*BEGIN DSRECORD', dsjob_content, re.DOTALL | re.MULTILINE)
            job_begin = "BEGIN DSJOB\n" + (job_begin_match.group(1).strip() + "\n" if job_begin_match else "")
            
            record_pattern = r'BEGIN DSRECORD.*?END DSRECORD'
            records = re.findall(record_pattern, dsjob_content, re.DOTALL | re.MULTILINE)
            sections = [rec.strip() + "\n" for rec in records if rec.strip()]
            
            if not sections:
                logging.warning(f"No DSRECORD sections found in {dsx_filename}, skipping")
                return
            
            job_end = "\nEND DSJOB"
            
            # Process sections
            processed_sections = []
            for idx, section in enumerate(sections, 1):
                prompt = f"Transform this Netezza DSX section to Oracle form. Return ONLY the transformed DSX section without any additional text, explanations, or wrappers:\n\n{section}"
                response = get_ai_response(prompt)
                
                # Clean the response to extract only the DSRECORD block
                dsrecord_match = re.search(r'BEGIN DSRECORD.*?END DSRECORD', response, re.DOTALL | re.MULTILINE | re.IGNORECASE)
                if dsrecord_match:
                    cleaned = dsrecord_match.group(0).strip() + "\n"
                else:
                    logging.warning(f"Could not extract DSRECORD from response for section {idx} in {dsx_filename}, using full response")
                    cleaned = response.strip() + "\n"
                indentation_preserved = preserve_indentation(section, cleaned)
                processed_sections.append(indentation_preserved)
                ti.xcom_push(key=f"transformed_record_{dsx_filename}_{idx}", value=indentation_preserved)
                logging.info(f"Processed section {idx}/{len(sections)} for {dsx_filename}")
            
            # Merge sections
            merged_content = dsx_header + job_begin + "".join(processed_sections) + job_end
            
            # Save transformed file with same filename
            transformed_path = os.path.join(repo_path, poc_dir, dsx_filename)
            with open(transformed_path, 'w', encoding='utf-8') as f:
                f.write(merged_content)
            
            ti.xcom_push(key="transformed_path", value=transformed_path)
            ti.xcom_push(key="repo_name", value= repo_name)
            ti.xcom_push(key="git_url", value=git_url)
            ti.xcom_push(key="dsx_filename", value=dsx_filename)
            logging.info(f"Transformed DSX file saved at {transformed_path}")
        
        except Exception as e:
            logging.error(f"Error transforming DSX file {dsx_filename}: {str(e)}")
            shutil.rmtree(repo_path)
            raise
    
    except Exception as e:
        logging.error(f"Error processing DSX file {dsx_filename}: {str(e)}")
        raise

def create_pr(ti, **context):
    try:
        repo_path = ti.xcom_pull(key="repo_path")
        transformed_path = ti.xcom_pull(key="transformed_path")
        repo_name = ti.xcom_pull(key="repo_name")
        dsx_filename = ti.xcom_pull(key="dsx_filename")
        gh_token = Variable.get("GITHUB_TOKEN")
        
        # Load processed files
        processed_files = load_processed_files()
        in_progress_files = load_in_progress_files()
        # Open repository
        repo = git.Repo(repo_path)
        
        # Create a unique branch
        branch_name = f"transform-{dsx_filename.replace('.dsx', '').replace('.', '_')}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        repo.git.checkout('-b', branch_name)
        
        # Add and commit the transformed file
        repo.index.add([transformed_path])
        repo.index.commit(f"Transformed {dsx_filename} from Netezza to Oracle")
        
        # Push the branch
        origin = repo.remote(name='origin')
        origin.push(branch_name)
        
        # Create PR
        g = Github(gh_token)
        gh_repo = g.get_repo(repo_name)
        pr = gh_repo.create_pull(
            title=f"Transformed DSX: {dsx_filename}",
            body=f"Automated transformation of {dsx_filename} from Netezza to Oracle using AI agent.",
            head=branch_name,
            base="main"
        )
        
        logging.info(f"Created PR #{pr.number} for {dsx_filename} in branch {branch_name}")
        
        # Update processed files
        processed_files.add(dsx_filename)
        save_processed_files(processed_files)

        # Remove from in-progress files
        in_progress_files.discard(dsx_filename)
        save_in_progress_files(in_progress_files)
        
        # Clean up temp dir
        shutil.rmtree(repo_path)
        logging.info(f"Cleaned up temp repo path: {repo_path}")
    
    except Exception as e:
        logging.error(f"Error creating PR for {dsx_filename}: {str(e)}")
        # Remove from in-progress files on failure to avoid getting stuck
        in_progress_files = load_in_progress_files()
        in_progress_files.discard(dsx_filename)
        save_in_progress_files(in_progress_files)
        shutil.rmtree(repo_path)
        raise

with DAG(
    "dsx_transform_file",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["dsx", "transformation", "netezza", "oracle"]
) as dag:
    
    transform_task = PythonOperator(
        task_id="transform_dsx_file",
        python_callable=transform_dsx_file,
        provide_context=True
    )
    
    create_pr_task = PythonOperator(
        task_id="create_pr",
        python_callable=create_pr,
        provide_context=True
    )
    
    transform_task >> create_pr_task