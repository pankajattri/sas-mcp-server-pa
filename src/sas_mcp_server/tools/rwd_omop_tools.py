# Copyright © 2025, SAS Institute Inc., Cary, NC, USA.  All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tier 11 — RWD / OMOP concept matching tools.

Runs Viya Job Execution jobs that map raw RWD codes/text to OMOP concepts.
Authentication uses the same ``get_token(ctx)`` path as every other tier — no
password grant, CLIENT_SECRET, or LOGON_TOKEN_URL of its own.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Annotated, Any

import httpx
from fastmcp import Context, FastMCP

from ..config import VIYA_ENDPOINT
from ..viya_client import logger
from ._common import coerce_json_dict, coerce_json_list, make_session_helpers

# Job definition IDs and related knobs (Viya-side configuration).
GET_UNIQUE_VALUES_JOB_ID = os.getenv("GET_UNIQUE_VALUES_JOB_ID", "")
CREATE_SDTM_SCHEMA_DESCRIPTION_JOB_ID = os.getenv(
    "CREATE_SDTM_SCHEMA_DESCRIPTION_JOB_ID", ""
)
CREATE_METADATA_FOR_LINEAGE_JOB_ID = os.getenv("CREATE_METADATA_FOR_LINEAGE_JOB_ID", "")
CREATE_RWD_SCHEMA_DESCRIPTION_JOB_ID = os.getenv(
    "CREATE_RWD_SCHEMA_DESCRIPTION_JOB_ID", ""
)
RUN_ADHOC_CODE_JOB_ID = os.getenv("RUN_ADHOC_CODE_JOB_ID", "")
GET_TABLE_AND_COLUMN_DETAILS_JOB_ID = os.getenv(
    "GET_TABLE_AND_COLUMN_DETAILS_JOB_ID", ""
)
GET_ALTERNATE_CONCEPTS_JOB_ID = os.getenv("GET_ALTERNATE_CONCEPTS_JOB_ID", "")
RWD_SCHEMA_COLUMN_LIST = os.getenv(
    "RWD_SCHEMA_COLUMN_LIST", "columnName columnLabel actualDataType columnSemanticID"
)
MAX_WAIT_TIME = int(os.getenv("MAX_WAIT_TIME", "500"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))
LAST_N_LINES = int(os.getenv("LAST_N_LINES", "500"))
OMOP_TABLES_JSON = os.getenv("OMOP_TABLES_JSON", "tables.json")

VALIDATOR_BATCH_PROMPT = """
You are a strict OMOP CDM ETL Mapping Validator. You have NO knowledge of any previous prompts or step numbers. You will be given only the generated mapping JSON and the target OMOP table name.

Your job is to independently verify that the mapping rules strictly follow these exact requirements (these are the only rules you check):

**RULE A - Clinical Concept Pair Rule** (for every field ending with _concept_id that has a matching _source_concept_id, e.g. condition_concept_id / condition_source_concept_id, drug_concept_id / drug_source_concept_id, etc.):
The mapping_rule for the STANDARD concept field (*_concept_id) MUST contain the exact phrase:
"Use TARGET_CONCEPT_ID ONLY where TARGET_STANDARD_CONCEPT == 'S'"
AND it must explicitly mention that the field defaults to 0 when no standard concept is found.

**RULE B - Metadata / Singleton Rule** (for every concept field that does NOT have a matching _source_concept_id, e.g. type_concept_id, condition_status_concept_id, route_concept_id, etc.):
The mapping_rule MUST explicitly state that it looks up the value in the table named {VOCABULARY_FILE_NAME} and uses a Standard Concept (TARGET_STANDARD_CONCEPT == 'S') from that table. Even if the mapping looks obvious, the rule must still mention performing this vocabulary lookup.

**RULE C - Domain Filter Rule** (applies only to fact tables: CONDITION_OCCURRENCE, DRUG_EXPOSURE, PROCEDURE_OCCURRENCE, MEASUREMENT, OBSERVATION, DEVICE_EXPOSURE, SPECIMEN, NOTE):
The mapping_rule for the primary clinical concept field (the main *_concept_id) MUST contain wording that is equivalent to:
"Filter records where TARGET_DOMAIN_ID matches the current table domain"
OR
"Filter records WHERE TARGET_DOMAIN_ID = '[EXPECTED_DOMAIN_ID]'"
where the expected domain is one of: Condition, Drug, Procedure, Measurement, Observation, Device, Specimen, Note (matching the current table).

**RULE D - Basic FK/PK Logic** (quick sanity check):
- Primary-key-style fields (person_id, visit_occurrence_id, etc.) must mention how uniqueness is ensured (direct mapping or surrogate generation).
- Foreign-key fields referencing other raw tables must mention the required join.

**Evaluation Rules:**
- Be strict. Minor wording differences are allowed only if the meaning is identical.
- If the required phrase or logic is missing or incorrect — rule_passed = false.
- Evaluate EVERY field in the provided JSON array.

**Output ONLY valid JSON** with this exact structure. Do not add any extra text, explanations, or markdown.

{{
  "field_reports": {{
    "variable_name_1": {{
      "rule_passed": boolean,
      "failed_checks": ["pair_rule", "singleton_rule", "domain_filter", "fk_pk_logic"],
      "remediation_instruction": "string describing exactly what is missing or wrong (null if rule_passed is true)"
    }}
  }}
}}
"""


def extract_sas_code_from_log(log_lines: list[str]) -> str:
    """Extract SAS code fragments echoed in a job log (MPRINT blocks)."""
    sas_code_lines: list[str] = []
    current_statement: list[str] = []
    in_mprint_block = False
    mprint_pattern = re.compile(r"normal: MPRINT\(([A-Za-z_][A-Za-z0-9_]*)\):\s*(.*)")
    normal_pattern = re.compile(r"normal:\s*(.*)")

    for line in log_lines:
        line = line.strip()
        mprint_match = mprint_pattern.match(line)
        if mprint_match:
            in_mprint_block = True
            code_fragment = mprint_match.group(2).strip()
            if code_fragment:
                current_statement.append(code_fragment)
            if code_fragment.endswith(";"):
                sas_code_lines.extend(current_statement)
                current_statement = []
                in_mprint_block = False
            continue
        if in_mprint_block:
            normal_match = normal_pattern.match(line)
            if normal_match:
                code_fragment = normal_match.group(1).strip()
                if code_fragment:
                    current_statement.append(code_fragment)
                    if code_fragment.endswith(";"):
                        sas_code_lines.extend(current_statement)
                        current_statement = []
                        in_mprint_block = False
            else:
                if current_statement and current_statement[-1].endswith(";"):
                    sas_code_lines.extend(current_statement)
                    current_statement = []
                in_mprint_block = False
    if current_statement and current_statement[-1].endswith(";"):
        sas_code_lines.extend(current_statement)
    return "\n".join(str(item) for item in sas_code_lines)


def extract_result_urls(data_dict: dict[str, Any]) -> tuple[
    str | None, str | None, str | None, str | None, str | None
]:
    """Resolve log, listing, html, rtf, and json result content URLs."""
    log_file_url = text_output_file_url = html_file_url = other_report_format_url = (
        json_format_url
    ) = None
    base = VIYA_ENDPOINT.rstrip("/")
    for key, value in data_dict.items():
        if key.endswith(".log.txt"):
            log_file_url = f"{base}{value}/content"
        elif key.endswith(".list.txt") or key.endswith(".listing.txt"):
            text_output_file_url = f"{base}{value}/content"
        elif key.endswith(".html"):
            html_file_url = f"{base}{value}/content"
        elif key.endswith(".rtf"):
            other_report_format_url = f"{base}{value}/content"
        elif key.endswith(".json"):
            json_format_url = f"{base}{value}/content"
    return (
        log_file_url,
        text_output_file_url,
        html_file_url,
        other_report_format_url,
        json_format_url,
    )


def format_table_col_mapping_for_job(table_columns: dict[str, Any]) -> str:
    """Turn ``{"Table1": ["col_a", "col_b"], ...}`` into job mapping text."""
    segments = []
    for table_name, cols in table_columns.items():
        if cols is None:
            continue
        if isinstance(cols, str):
            cols = [cols]
        if not isinstance(cols, (list, tuple)):
            continue
        names = [str(c).strip() for c in cols if str(c).strip()]
        if not names:
            continue
        segments.append(f"{table_name}:{' '.join(names)}")
    return " | ".join(segments)


async def create_file_async(
    client: httpx.AsyncClient,
    file_name: str,
    create_file_url: str,
    file_content: str,
    parent_folder_uri: str | None = None,
) -> httpx.Response:
    """Upload text as a file in Viya Files service."""
    logger.info("Trying to create file %s", (file_content or "")[:200])
    files = {
        "filename": (None, file_name),
        "file": (file_name, file_content, "text/plain"),
    }
    headers = {
        "Accept": "application/json, application/vnd.sas.file+json",
    }
    if parent_folder_uri is None:
        return await client.post(create_file_url, files=files, headers=headers)
    return await client.post(
        create_file_url,
        files=files,
        headers=headers,
        params={"parentFolderUri": parent_folder_uri},
    )


async def fetch_url_content_async(client: httpx.AsyncClient, url: str | None) -> str | None:
    """Fetch raw content from a Viya file content URL."""
    if url is None:
        return None
    headers = {
        "Delegate-Domain": "",
        "Accept": (
            "application/json, application/vnd.sas.job.execution.job+json, "
            "application/vnd.sas.error+json"
        ),
    }
    try:
        response = await client.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        return (
            f"Failed to fetch URL content for URL {url}, "
            f"status code: {response.status_code}"
        )
    except httpx.RequestError as exc:
        return f"Error fetching URL content: {exc}"


async def run_and_monitor_job_async(
    client: httpx.AsyncClient,
    job_id: str,
    url: str,
    **additional_args: Any,
) -> tuple[Any, dict[str, Any]]:
    """Submit a pre-defined SAS job and poll until it finishes."""
    response_job_status = SimpleNamespace(text="not_submitted")

    valid_job_completion_statuses = ["completed"]
    multi_terminal_jobs = [
        jid
        for jid in (
            CREATE_SDTM_SCHEMA_DESCRIPTION_JOB_ID,
            CREATE_METADATA_FOR_LINEAGE_JOB_ID,
            CREATE_RWD_SCHEMA_DESCRIPTION_JOB_ID,
            GET_UNIQUE_VALUES_JOB_ID,
            GET_TABLE_AND_COLUMN_DETAILS_JOB_ID,
            GET_ALTERNATE_CONCEPTS_JOB_ID,
        )
        if jid
    ]
    if job_id in multi_terminal_jobs:
        valid_job_completion_statuses = ["completed", "failed"]

    sas_code: Any = []
    result: Any = "Error: Could not run Job. Contact your system administrator."
    payload = {
        "name": "Run Job",
        "description": "Run sas Job",
        "jobDefinitionUri": f"/jobDefinitions/definitions/{job_id}",
        "arguments": {**additional_args},
    }
    headers_json = {
        "Delegate-Domain": "",
        "Content-Type": "application/json",
        "Accept": (
            "application/json, application/vnd.sas.job.execution.job+json, "
            "application/vnd.sas.error+json"
        ),
    }
    if isinstance(payload.get("arguments"), str):
        try:
            payload["arguments"] = json.loads(payload["arguments"])
        except json.JSONDecodeError:
            pass

    submit_job_response = await client.post(url, json=payload, headers=headers_json)
    log_url = text_output_file_url = html_url = other_report_format_url = json_format_url = (
        None
    )
    exec_url = f"{VIYA_ENDPOINT.rstrip('/')}/jobExecution/jobs"
    if submit_job_response.status_code != 201:
        err_body = getattr(submit_job_response, "text", "") or ""
        logger.error(
            "Job submit failed: status=%s body=%s",
            submit_job_response.status_code,
            err_body[:2000],
        )
        return (
            f"Error: Job submit failed ({submit_job_response.status_code}): "
            f"{err_body[:1500]}",
            {
                "log_url": None,
                "html_url": None,
                "otherReportFormatURL": None,
                "sas_code": [],
                "job_status": "submit_failed",
                "textOutputFileURL": None,
                "jsonFileURL": None,
            },
        )

    submit_job_id = submit_job_response.json()["id"]
    exec_url = f"{exec_url}/{submit_job_id}"
    headers = {
        "Delegate-Domain": "",
        "Accept": (
            "application/json, application/vnd.sas.job.execution.job+json, "
            "application/vnd.sas.error+json"
        ),
    }
    start_time = time.time()
    while True:
        if time.time() - start_time >= MAX_WAIT_TIME:
            result = "Taking too long. Time out. Aborted."
            break
        url_job_status = f"{exec_url}/state"
        headers_job_status = {"Accept": "text/plain"}
        response_job_status = await client.get(
            url_job_status, headers=headers_job_status
        )
        if response_job_status.status_code == 200 and response_job_status.text in (
            "failed",
            "completed",
            "canceled",
        ):
            job_response = await client.get(exec_url, headers=headers)
            job_results = job_response.json().get("results", {})
            (
                log_url,
                text_output_file_url,
                html_url,
                other_report_format_url,
                json_format_url,
            ) = extract_result_urls(job_results)
            logger.info(
                "Job execution results: status=%s text_output=%s json=%s",
                response_job_status.text,
                text_output_file_url,
                json_format_url,
            )
            if (
                text_output_file_url is not None
                and response_job_status.text in valid_job_completion_statuses
            ):
                try:
                    output_response = await client.get(
                        text_output_file_url, headers=headers
                    )
                    logresponse = None
                    if log_url:
                        logresponse = await client.get(log_url, headers=headers)
                    if output_response.status_code == 200:
                        result = output_response.text
                    else:
                        result = (
                            "Failed to fetch output, status code: "
                            f"{output_response.status_code}"
                        )
                    if logresponse is not None and logresponse.status_code == 200:
                        log_lines = logresponse.text.splitlines()
                        sas_code = extract_sas_code_from_log(log_lines)
                except httpx.RequestError as exc:
                    result = f"Error fetching output: {exc}"
            elif log_url is not None:
                try:
                    logresponse = await client.get(log_url, headers=headers)
                    if logresponse.status_code == 200:
                        log_lines = logresponse.text.splitlines()
                        lines: list[str] = []
                        capture = False
                        for line in log_lines:
                            if line.startswith("source:") and (
                                "%runCode(&folderPath, &fileURI, &fileName);" in line
                            ):
                                capture = True
                            if capture and line.lower().startswith(
                                ("error:", "warning:", "source:")
                            ):
                                lines.append(line)
                        result = "\n".join(lines[-LAST_N_LINES:])
                    else:
                        result = (
                            "Failed to fetch logs, status code: "
                            f"{logresponse.status_code}"
                        )
                except httpx.RequestError as exc:
                    result = f"Error fetching logs: {exc}"
            break
        await asyncio.sleep(POLL_INTERVAL)

    log_text, html_text, text_out_text, json_text = await asyncio.gather(
        fetch_url_content_async(client, log_url),
        fetch_url_content_async(client, html_url),
        fetch_url_content_async(client, text_output_file_url),
        fetch_url_content_async(client, json_format_url),
    )

    return result, {
        "log_url": log_text,
        "html_url": html_text,
        "otherReportFormatURL": other_report_format_url,
        "sas_code": sas_code,
        "job_status": response_job_status.text,
        "textOutputFileURL": text_out_text,
        "jsonFileURL": json_text,
    }


async def run_sas_code_async(
    client: httpx.AsyncClient,
    code_to_run: str,
    file_name: str | None = "sascode.sas",
) -> tuple[Any, dict[str, Any]]:
    """Upload SAS/SQL as a file and execute it via ``RUN_ADHOC_CODE_JOB_ID``."""
    empty_meta = {
        "log_url": None,
        "html_url": None,
        "otherReportFormatURL": None,
        "sas_code": code_to_run,
        "job_status": "failed",
        "textOutputFileURL": None,
        "jsonFileURL": None,
    }
    if not file_name or not str(file_name).strip():
        return (
            "Error: The following mandatory parameters are missing or empty:\n"
            "fileName\n"
            "Please provide values for these parameters to run code.\n"
            "Required parameters:\n"
            "- fileName: Name of the file containing SAS code.\n",
            empty_meta,
        )
    if not RUN_ADHOC_CODE_JOB_ID:
        return "RUN_ADHOC_CODE_JOB_ID is not configured.", empty_meta
    if not VIYA_ENDPOINT:
        return "VIYA_ENDPOINT is not configured.", empty_meta

    create_file_url = f"{VIYA_ENDPOINT.rstrip('/')}/files/files/"
    create_file_response = await create_file_async(
        client, "sascode.sas", create_file_url, code_to_run
    )
    result: Any = "Could not run Job. Contact your system administrator."
    dict_: dict[str, Any] = empty_meta
    if create_file_response.status_code == 201:
        run_code_url = f"{VIYA_ENDPOINT.rstrip('/')}/jobExecution/jobs"
        file_id = create_file_response.json()["id"]
        result, dict_ = await run_and_monitor_job_async(
            client,
            RUN_ADHOC_CODE_JOB_ID,
            run_code_url,
            folderPath="",
            fileURI=f"/files/files/{file_id}",
            fileName="sascode.sas",
            _omitSessionResults="false",
        )
    logger.info(
        "run_sas_code_async finished: job_status=%s",
        (dict_ or {}).get("job_status"),
    )
    return result, dict_


def parse_job_json_result(result_text: str, job_meta: dict[str, Any]) -> Any:
    """Prefer the JSON file payload from the job; fall back to listing body."""
    candidates: list[Any] = []
    if isinstance(job_meta, dict):
        candidates.append(job_meta.get("jsonFileURL"))
    candidates.append(result_text)
    for candidate in candidates:
        if not candidate or not isinstance(candidate, str):
            continue
        stripped = candidate.strip()
        if len(stripped) == 0 or stripped[0] not in "[{":
            continue
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            continue
    return None


def normalize_search_terms_for_job(
    search_terms: dict[str, Any],
) -> dict[str, list[str]]:
    """Coerce MCP input into ``{term: [alternate, ...], ...}``."""
    out: dict[str, list[str]] = {}
    if not isinstance(search_terms, dict):
        return out
    for key, value in search_terms.items():
        sk = str(key).strip()
        if not sk:
            continue
        if isinstance(value, str):
            lst = [s for s in [value.strip()] if s]
        elif isinstance(value, (list, tuple)):
            lst = [str(x).strip() for x in value if str(x).strip()]
        else:
            continue
        if lst:
            out[sk] = lst
    return out


async def get_unique_values_for_table_columns(
    client: httpx.AsyncClient,
    table_columns: dict[str, list[str]],
    base_lib: str,
) -> Any:
    """Run the configured SAS job for distinct / sample column values."""
    logger.info(
        "get_unique_values_for_table_columns: table_columns=%s base_lib=%s",
        table_columns,
        base_lib,
    )
    if not GET_UNIQUE_VALUES_JOB_ID:
        return {"error": "GET_UNIQUE_VALUES_JOB_ID is not configured."}
    if not VIYA_ENDPOINT:
        return {"error": "VIYA_ENDPOINT is not configured."}

    table_col_mapping = format_table_col_mapping_for_job(table_columns)
    if not table_col_mapping.strip():
        return {
            "error": (
                "table_columns must include at least one table with a "
                "non-empty column list."
            )
        }

    run_code_url = f"{VIYA_ENDPOINT.rstrip('/')}/jobExecution/jobs"
    try:
        result_text, job_meta = await run_and_monitor_job_async(
            client,
            GET_UNIQUE_VALUES_JOB_ID,
            run_code_url,
            table_col_mapping=table_col_mapping,
            display_cols=RWD_SCHEMA_COLUMN_LIST,
            privacy_columns_CIP_values="sensitive private",
            column_types_no_unique="DATE GENERIC_ID ORGANIZATIONID",
            m=500,
            base_lib=base_lib,
            _omitSessionResults="false",
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("get_unique_values_for_table_columns job failed")
        return {"error": str(exc)}

    status = (job_meta or {}).get("job_status", "unknown")
    if status == "submit_failed":
        return {
            "error": "Job could not be submitted to Viya.",
            "job_status": status,
            "details": result_text[:4000] if isinstance(result_text, str) else result_text,
        }
    if status not in ("completed", "failed"):
        return {
            "error": "Job did not finish successfully.",
            "job_status": status,
            "details": result_text[:4000] if isinstance(result_text, str) else result_text,
        }

    parsed = parse_job_json_result(result_text, job_meta or {})
    if parsed is not None:
        return parsed
    return {
        "error": "Could not parse JSON from job output.",
        "job_status": status,
        "listing_preview": (result_text or "")[:4000],
        "json_preview": str((job_meta or {}).get("jsonFileURL", ""))[:4000],
    }


def register(mcp: FastMCP, get_token: Callable[[Context], Awaitable[str]]) -> None:
    """Register Tier 11 (RWD / OMOP) tools on *mcp*."""

    viya_session, _ = make_session_helpers(get_token)

    @mcp.tool()
    async def perform_standard_concept_match(
        sql_code: str,
        names_of_files_generated: Annotated[list[str], "Output file names (no path)"],
        data_dir_path: str,
        ctx: Context,
    ) -> Any:
        """Run adhoc SAS/SQL that writes OMOP mapping files, then fetch table/column details.

        Uploads and executes ``sql_code`` via the configured adhoc job, then runs
        ``GET_TABLE_AND_COLUMN_DETAILS`` on the generated files and returns parsed JSON.

        Args:
            sql_code: SAS/SQL source to upload and execute.
            names_of_files_generated: Output file names (no path); passed to the
                details job as a space-separated list.
            data_dir_path: Directory path passed as ``dir_path`` to the details job.
        """
        names = coerce_json_list(names_of_files_generated)
        logger.info(
            "perform_standard_concept_match: files=%s dir=%s",
            names,
            data_dir_path,
        )
        if not VIYA_ENDPOINT:
            return {"error": "VIYA_ENDPOINT is not configured."}
        if not names:
            return {"error": "names_of_files_generated must contain at least one file name."}

        async with viya_session("perform_standard_concept_match", ctx) as client:
            run_output, run_meta = await run_sas_code_async(client, sql_code)
            if (run_meta or {}).get("job_status") != "completed":
                return {
                    "error": (
                        "Adhoc code job did not complete successfully; "
                        "table/column details job was not started."
                    ),
                    "job_status": (run_meta or {}).get("job_status"),
                    "run_output_preview": (run_output or "")[:4000],
                    "run_job_details": run_meta,
                }

            if not GET_TABLE_AND_COLUMN_DETAILS_JOB_ID:
                return {
                    "error": "GET_TABLE_AND_COLUMN_DETAILS_JOB_ID is not configured.",
                    "adhoc_output_preview": (run_output or "")[:2000],
                    "run_job_details": run_meta,
                }

            file_list = " ".join(str(n).strip() for n in names if str(n).strip())
            if not file_list:
                return {
                    "error": (
                        "names_of_files_generated contained no non-empty file "
                        "names after trimming."
                    )
                }

            run_code_url = f"{VIYA_ENDPOINT.rstrip('/')}/jobExecution/jobs"
            try:
                details_text, details_meta = await run_and_monitor_job_async(
                    client,
                    GET_TABLE_AND_COLUMN_DETAILS_JOB_ID,
                    run_code_url,
                    dir_path=data_dir_path,
                    file_list=file_list,
                    number_of_rows="500",
                    _omitSessionResults="false",
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("GET_TABLE_AND_COLUMN_DETAILS job failed")
                return {
                    "error": str(exc),
                    "adhoc_output_preview": (run_output or "")[:2000],
                    "run_job_details": run_meta,
                }

            status = (details_meta or {}).get("job_status", "unknown")
            if status == "submit_failed":
                return {
                    "error": "Table/column details job could not be submitted.",
                    "job_status": status,
                    "details": (details_text or "")[:4000],
                    "run_job_details": run_meta,
                }
            if status not in ("completed", "failed"):
                return {
                    "error": "Table/column details job did not finish.",
                    "job_status": status,
                    "details": (details_text or "")[:4000],
                    "run_job_details": run_meta,
                }

            parsed = parse_job_json_result(details_text, details_meta or {})
            if parsed is not None:
                return parsed
            return {
                "error": "Could not parse JSON from table/column details job output.",
                "job_status": status,
                "listing_preview": (details_text or "")[:4000],
                "json_preview": str((details_meta or {}).get("jsonFileURL", ""))[:4000],
                "run_job_details": run_meta,
            }

    @mcp.tool()
    async def get_alternate_concepts_for_unmatched_terms(
        search_terms: Annotated[
            dict[str, list[str]],
            "Map unmatched terms to alternate search strings, e.g. "
            "{'glucose': ['blood sugar', 'HbA1c']}",
        ],
        target_table_domain: Annotated[
            str, "Clinical domain, e.g. Measurement, Observation, Drug"
        ],
        out_dir: Annotated[str, "Directory path for SAS job output"],
        table_name: Annotated[str, "Source table name being processed"],
        ctx: Context,
    ) -> Any:
        """Run the GET_ALTERNATE_CONCEPTS job to resolve unmatched clinical terms.

        Args:
            search_terms: Object mapping each unmatched string to alternate searches.
            target_table_domain: OMOP domain filter for the search.
            out_dir: Output directory for the job.
            table_name: Source table name (used in output file names).
        """
        terms = coerce_json_dict(search_terms)
        logger.info(
            "get_alternate_concepts_for_unmatched_terms: domain=%s out_dir=%s table=%s",
            target_table_domain,
            out_dir,
            table_name,
        )
        if not VIYA_ENDPOINT:
            return {"error": "VIYA_ENDPOINT is not configured."}
        if not GET_ALTERNATE_CONCEPTS_JOB_ID:
            return {"error": "GET_ALTERNATE_CONCEPTS_JOB_ID is not configured."}
        if not (out_dir or "").strip():
            return {"error": "out_dir must be non-empty."}

        normalized = normalize_search_terms_for_job(terms)
        if not normalized:
            return {
                "error": (
                    "search_terms must be a non-empty object with at least one "
                    "valid key and alternate list."
                )
            }

        search_terms_json = json.dumps(normalized, ensure_ascii=False)
        run_code_url = f"{VIYA_ENDPOINT.rstrip('/')}/jobExecution/jobs"
        async with viya_session(
            "get_alternate_concepts_for_unmatched_terms", ctx
        ) as client:
            try:
                result_text, job_meta = await run_and_monitor_job_async(
                    client,
                    GET_ALTERNATE_CONCEPTS_JOB_ID,
                    run_code_url,
                    json_input=search_terms_json.replace("\n", "").strip(),
                    target_table_domain=target_table_domain,
                    out_path=out_dir,
                    out_dir=out_dir,
                    out_filename=f"{table_name}_alternate_concepts_search_terms",
                    out_file=f"{table_name}_search_term_results",
                    _omitSessionResults="false",
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("GET_ALTERNATE_CONCEPTS job failed")
                return {"error": str(exc)}

            status = (job_meta or {}).get("job_status", "unknown")
            if status == "submit_failed" or status not in ("completed", "failed"):
                return {
                    "job_status": status,
                    "response_preview": (result_text or "")[:200],
                }

            parsed = parse_job_json_result(result_text, job_meta or {})
            if parsed is not None:
                preview_src = json.dumps(parsed, ensure_ascii=False)
            else:
                preview_src = result_text or str(
                    (job_meta or {}).get("jsonFileURL", "") or ""
                )
            return {
                "job_status": status,
                "response_preview": preview_src[:2000],
            }

    @mcp.tool()
    async def validate_omop_mapping(
        table_name: str,
        proposed_mappings: Annotated[
            list[dict[str, Any]], "Mapping objects with variable / mapping_rule fields"
        ],
        ctx: Context,
    ) -> dict[str, Any]:
        """Validate proposed OMOP CDM field mappings against a local table schema.

        Loads ``tables.json`` (or the path in ``OMOP_TABLES_JSON``) and checks
        required fields, empty rules, and SOURCE_TO_STANDARD_MAPPING lookup flags.
        Optional LLM batch scoring runs only when an OpenAI-compatible client is
        available via ``OPENAI_API_KEY``; otherwise local results are returned.

        Args:
            table_name: Target OMOP table (e.g. ``CONDITION_OCCURRENCE``).
            proposed_mappings: List of mapping dicts with at least ``variable``
                and ``mapping_rule``.
        """
        _ = ctx  # same auth surface as other tools; no Viya call for local checks
        mappings = coerce_json_list(proposed_mappings)
        schema_path = Path(OMOP_TABLES_JSON)
        if not schema_path.is_file():
            return {
                "error": (
                    f"OMOP schema file not found at '{schema_path}'. "
                    "Set OMOP_TABLES_JSON to the tables.json path."
                )
            }
        with schema_path.open(encoding="utf-8") as handle:
            schema = json.load(handle)

        table_upper = table_name.upper()
        table_meta = schema.get(table_upper)
        if not table_meta:
            return {"error": "Table not found"}

        domain_map = {
            "CONDITION_OCCURRENCE": "Condition",
            "DRUG_EXPOSURE": "Drug",
            "PROCEDURE_OCCURRENCE": "Procedure",
            "MEASUREMENT": "Measurement",
            "OBSERVATION": "Observation",
            "DEVICE_EXPOSURE": "Device",
        }
        expected_domain = domain_map.get(table_upper, "N/A")
        field_requirements = table_meta.get("Fields", [])
        mapped_dict = {
            str(m.get("variable", "")).lower(): m
            for m in mappings
            if isinstance(m, dict)
        }

        results: dict[str, Any] = {
            "table": table_upper,
            "All_required_fields_present": True,
            "missing_required_fields": [],
            "incorrect_lookup_flags": [],
            "batch_report": {},
        }
        validation_payload: list[dict[str, Any]] = []

        for field in field_requirements:
            fname = field["CDM Field"]
            mapping = mapped_dict.get(fname.lower())
            has_valid_rule = mapping and str(mapping.get("mapping_rule", "")).strip()

            if field.get("Required") == "Yes" and not has_valid_rule:
                results["All_required_fields_present"] = False
                results["missing_required_fields"].append(fname)
                continue

            if mapping and not has_valid_rule:
                results["batch_report"][fname] = {
                    "rule_passed": False,
                    "remediation_instruction": (
                        "The mapping_rule is empty. Please provide the derivation logic."
                    ),
                }
                continue

            if has_valid_rule:
                if (
                    field.get("FK Table") == "SOURCE_TO_STANDARD_MAPPING"
                    and mapping.get("additional_lookup_needed") != "Yes"
                ):
                    results["incorrect_lookup_flags"].append(fname)

                validation_payload.append(
                    {
                        "field": fname,
                        "mapping_rule": mapping.get("mapping_rule"),
                        "is_primary_clinical": (
                            fname.lower()
                            == f"{table_upper.split('_')[0].lower()}_concept_id"
                        ),
                        "metadata": {
                            "FK_Table": field.get("FK Table"),
                            "Is_PK": field.get("Primary Key") == "Yes",
                            "Expected_Domain": expected_domain,
                        },
                    }
                )

        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not validation_payload:
            return results
        if not api_key:
            results["batch_report_note"] = (
                "LLM batch validation skipped (OPENAI_API_KEY not set). "
                "Local required-field and lookup checks are included above."
            )
            return results

        try:
            from openai import OpenAI  # type: ignore[import-not-found]
        except ImportError:
            results["batch_report_note"] = (
                "LLM batch validation skipped (openai package not installed)."
            )
            return results

        def _call_llm() -> Any:
            llm = OpenAI(api_key=api_key)
            return llm.chat.completions.create(
                model=os.getenv("OMOP_VALIDATOR_MODEL", "gpt-4o"),
                messages=[
                    {"role": "system", "content": VALIDATOR_BATCH_PROMPT},
                    {"role": "user", "content": json.dumps(validation_payload)},
                ],
                response_format={"type": "json_object"},
            )

        response = await asyncio.to_thread(_call_llm)
        results["batch_report"] = json.loads(
            response.choices[0].message.content
        ).get("field_reports", {})
        return results
