#' pinsExtras: Additional boards for pins
#'
#' @description Extra board implementations for the `pins` package.
#' @keywords internal
#' @importFrom pins board_cache_path pin_exists pin_versions pin_read pin_write
#' @importFrom pins board_deparse new_board pin_download pin_upload pin_delete pin_fetch pin_list pin_meta pin_store pin_version_delete write_board_manifest write_board_manifest_yaml
#' @importFrom DBI dbGetQuery
#' @importFrom odbc odbc
#' @importFrom cli cli_abort
#' @importFrom fs path path_abs path_dir path_file file_exists dir_create path_expand
#' @importFrom rlang expr is_string check_dots_used check_dots_empty `%||%`
#' @importFrom purrr compact map_chr
#' @importFrom digest digest
#' @importFrom tibble tibble
#' @importFrom generics required_pkgs
#' @importFrom withr local_tempfile local_tempdir
#' @import yaml
"_PACKAGE"
