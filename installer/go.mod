module github.com/colony-2/pgwf-go/installer

go 1.24.1

require (
	github.com/colony-2/pgwf-go v0.0.0
	github.com/fergusstrange/embedded-postgres v1.32.0
	github.com/lib/pq v1.10.9
)

require (
	github.com/colony-2/pgwf v0.0.0-20251119021943-aefb37b99bf4 // indirect
	github.com/xi2/xz v0.0.0-20171230120015-48954b6210f8 // indirect
)

replace github.com/colony-2/pgwf-go => ..
