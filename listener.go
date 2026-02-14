package hydro

type Change[T any] interface {
	Stack(c Change[T]) Change[T]
}
