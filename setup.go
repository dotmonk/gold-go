package gold

var registrars []func(*App)

func RegisterAPIFunc(fn func(*App)) {
	registrars = append(registrars, fn)
}

func RegisterAll(app *App) {
	for _, fn := range registrars {
		fn(app)
	}
}
