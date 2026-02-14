package hydro

// Get a ListenerDictionary from an object from the context, this makes it easier to work with Hydro's transactions
func CtxDict[T any, C Change[C]](c *Context, dict *ListenerDictionary[T, C]) *ListenerDictionary[T, C] {
	return c.objects[dict.GetIdentifier()].Object.(*ListenerDictionary[T, C])
}

func CtxDictGetKeys[T any, C Change[C]](c *Context, dict *ListenerDictionary[T, C]) []string {
	return c.objects[dict.GetIdentifier()].lockedKeys
}

// Set a value in the context for a listener dictionary
func CtxDictSet[T any, C Change[C]](c *Context, dict *ListenerDictionary[T, C], key string, value C) {
	if c.objects[dict.GetIdentifier()].changes == nil {
		c.objects[dict.GetIdentifier()].changes = map[string]any{}
	}

	c.objects[dict.GetIdentifier()].changes[key] = value
}

// A handy wrapper around Hydro's transactions system to handle a transaction on exactly one listener
func (ld *ListenerDictionary[T, C]) Transaction(keys []string, transaction func([]string) (map[string]C, error)) error {
	return Tx([]*TxObject{
		{
			Object: ld,
			Keys:   keys,
		},
	}, func(ctx *Context) error {
		results, err := transaction(CtxDictGetKeys(ctx, ld))
		if err != nil {
			return err
		}

		// Set the results in the map
		for key, result := range results {
			CtxDictSet(ctx, ld, key, result)
		}
		return nil
	})
}
