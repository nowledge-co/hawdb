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

from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError

import hawdb_dify


class HawDBProvider(ToolProvider):
    """Checks the database the tools will open when the credentials are saved."""

    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        try:
            hawdb_dify.check_database(hawdb_dify.database_config(credentials))
        except Exception as error:
            raise ToolProviderCredentialValidationError(
                f"{type(error).__name__}: {error}"
            ) from error
