# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any

class Error(Exception):
    """Base error raised by HawDB."""

class ParseError(Error):
    """Statement failed to parse."""

class SemanticError(Error):
    """Statement failed semantic checks."""

class StorageError(Error):
    """Storage layer failure."""

    # Some failures carry `kind`, such as "constraint" or "schema" for a
    # relational refusal, and `retryable`.
    def __getattr__(self, name: str) -> Any: ...

class IntegrityError(StorageError):
    """Storage integrity violation."""

class DescriptorError(StorageError):
    """File descriptor budget or storage descriptor failure."""

class ExecutionError(Error):
    """Statement failed during execution."""

    # Some failures carry `kind` and `retryable`; resource refusals carry
    # their budget fields.
    def __getattr__(self, name: str) -> Any: ...

class ResourceBudgetError(ExecutionError):
    """Execution resource reservation refused; attributes identify the effective budget."""

class ConflictError(Error):
    """Transaction conflict or exhausted commit sequence."""

class CapabilityError(Error):
    """A required runtime capability is unavailable."""

class BranchError(Error):
    """Branch command is unsupported or the branch is busy."""

class AdmissionError(Error):
    """Query admission was rejected by runtime resource governance."""

    # The refused resource, for example "cpu_saturated" or "memory_saturated".
    code: str
    # True for a transient shortage that a later attempt can pass.
    retryable: bool

class TaskStoppedError(Error):
    """The runtime task was stopped before producing a result."""

class RetainedError(Error):
    """Experimental retained-interface failure; kind and retryable preserve the outcome."""

    def __getattr__(self, name: str) -> Any: ...

class BackpressureError(RetainedError):
    """Release held views and retry; the source has not advanced."""
