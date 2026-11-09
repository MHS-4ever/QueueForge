variable "enable_fault_injection" {
  description = "Enable deterministic faults only for incoming/__queueforge_faults__/ test keys."
  type        = bool
  default     = false
}
