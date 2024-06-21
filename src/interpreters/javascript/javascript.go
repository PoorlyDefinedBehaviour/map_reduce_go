package javascript

import (
	"fmt"

	"rogchap.com/v8go"
)

type Script struct {
	v8Context   *v8go.Context
	scriptValue *v8go.Value
}

func newScript(input string) (*Script, error) {
	isolate := v8go.NewIsolate()
	v8Context := v8go.NewContext(isolate)

	scriptValue, err := v8Context.RunScript(input, "script.js")
	if err != nil {
		v8Context.Close()
		return nil, fmt.Errorf("running script: %w", err)
	}

	return &Script{v8Context: v8Context, scriptValue: scriptValue}, nil
}

func (script *Script) Map(key, value string, emit func(key, value string)) error {
	mapFunction, err := script.v8Context.RunScript("map", "script.js")
	if err != nil {
		return fmt.Errorf("fetching map function from js: %w", err)
	}
	defer mapFunction.Release()

	fn, err := mapFunction.AsFunction()
	if err != nil {
		return fmt.Errorf("casting value to js function: %w", err)
	}

	iso := script.v8Context.Isolate()

	emitFunction := v8go.NewFunctionTemplate(iso, func(info *v8go.FunctionCallbackInfo) *v8go.Value {
		key := info.Args()[0].String()
		value := info.Args()[1].String()

		emit(key, value)

		return v8go.Undefined(iso)
	})

	keyValue, err := v8go.NewValue(iso, key)
	if err != nil {
		return fmt.Errorf("creating js value from key: key=%s %w", key, err)
	}
	defer keyValue.Release()

	valueValue, err := v8go.NewValue(iso, value)
	if err != nil {
		return fmt.Errorf("creating js value from value: value=%s %w", value, err)
	}
	defer valueValue.Release()

	result, err := fn.Call(v8go.Undefined(iso), keyValue, valueValue, emitFunction.GetFunction(script.v8Context))
	if err != nil {
		return fmt.Errorf("calling js map function: %w", err)
	}
	defer result.Release()

	return nil
}

func (script *Script) Reduce(key string, nextValueIter func() (string, bool), emit func(key, value string)) error {
	reduceFunction, err := script.v8Context.RunScript("reduce", "script.js")
	if err != nil {
		return fmt.Errorf("fetching reduce function from js: %w", err)
	}
	defer reduceFunction.Release()

	fn, err := reduceFunction.AsFunction()
	if err != nil {
		return fmt.Errorf("casting value to js function: %w", err)
	}

	iso := script.v8Context.Isolate()

	emitFunction := v8go.NewFunctionTemplate(iso, func(info *v8go.FunctionCallbackInfo) *v8go.Value {
		key := info.Args()[0].String()
		value := info.Args()[1].String()

		emit(key, value)

		return v8go.Undefined(iso)
	})

	nextValueFunction := v8go.NewFunctionTemplate(iso, func(_ *v8go.FunctionCallbackInfo) *v8go.Value {
		value, done := nextValueIter()

		arrayValue, err := script.v8Context.RunScript("new Array()", "")
		if err != nil {
			panic(err)
		}
		obj := arrayValue.Object()

		err = obj.Set("length", "2")
		if err != nil {
			panic(err)
		}

		if err := obj.SetIdx(0, value); err != nil {
			panic(err)
		}
		if err := obj.SetIdx(1, done); err != nil {
			panic(err)
		}

		return obj.Value
	})

	keyValue, err := v8go.NewValue(iso, key)
	if err != nil {
		return fmt.Errorf("creating js value from key: key=%s %w", key, err)
	}
	defer keyValue.Release()

	result, err := fn.Call(
		v8go.Undefined(iso),
		keyValue,
		nextValueFunction.GetFunction(script.v8Context),
		emitFunction.GetFunction(script.v8Context),
	)
	if err != nil {
		return fmt.Errorf("calling js map function: %w", err)
	}
	defer result.Release()

	return nil

}

func (script *Script) Close() {
	script.scriptValue.Release()
	script.v8Context.Close()
	script.v8Context.Isolate().Dispose()
}

func Parse(input string) (*Script, error) {
	script, err := newScript(input)
	if err != nil {
		return script, fmt.Errorf("instantiating javascripts script: %w", err)
	}

	return script, nil
}