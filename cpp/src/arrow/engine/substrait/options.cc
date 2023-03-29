// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/engine/substrait/options.h"

#include <google/protobuf/util/json_util.h>
#include <mutex>

#include "arrow/compute/exec/asof_join_node.h"
#include "arrow/compute/exec/options.h"
#include "arrow/engine/substrait/expression_internal.h"
#include "arrow/engine/substrait/options_internal.h"
#include "arrow/engine/substrait/relation_internal.h"
#include "substrait/extension_rels.pb.h"

namespace arrow {
namespace engine {

namespace {

template <typename T>
class ConfigurableSingleton {
 public:
  explicit ConfigurableSingleton(T new_value) : instance(std::move(new_value)) {}

  T Get() {
    std::lock_guard lk(mutex);
    return instance;
  }

  void Set(T new_value) {
    std::lock_guard lk(mutex);
    instance = std::move(new_value);
  }

 private:
  T instance;
  std::mutex mutex;
};

ConfigurableSingleton<std::shared_ptr<ExtensionProvider>>&
default_extension_provider_singleton() {
  static ConfigurableSingleton<std::shared_ptr<ExtensionProvider>> singleton(GetDefaultExtensionProvider());
  return singleton;
}

ConfigurableSingleton<NamedTapProvider>& default_named_tap_provider_singleton() {
  static ConfigurableSingleton<NamedTapProvider> singleton(
      [](const std::string& tap_kind, std::vector<compute::Declaration::Input> inputs,
         const std::string& tap_name,
         std::shared_ptr<Schema> tap_schema) -> Result<compute::Declaration> {
        return Status::NotImplemented(
            "Plan contained a NamedTapRel but no provider configured");
      });
  return singleton;
}

}  // namespace

std::shared_ptr<ExtensionProvider> default_extension_provider() {
  return default_extension_provider_singleton().Get();
}

void set_default_extension_provider(const std::shared_ptr<ExtensionProvider>& provider) {
  default_extension_provider_singleton().Set(provider);
}

NamedTapProvider default_named_tap_provider() {
  return default_named_tap_provider_singleton().Get();
}

void set_default_named_tap_provider(NamedTapProvider provider) {
  default_named_tap_provider_singleton().Set(std::move(provider));
}

}  // namespace engine
}  // namespace arrow
