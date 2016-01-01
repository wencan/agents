package internal

import (
	"io"
	"log"
)

func IoExchange(a, b io.ReadWriteCloser, done chan struct{}) (err error) {
	ch := make(chan error, 2)

	go ioCopyUntilError(a, b, ch)
	go ioCopyUntilError(b, a, ch)

	select {
	case err = <-ch:		//io error
		if pipe, ok := a.(*StreamPipe); ok {
			pipe.CloseWithError(err)
		} else {
			a.Close()
		}
		if pipe, ok := b.(*StreamPipe); ok {
			pipe.CloseWithError(err)
		} else {
			b.Close()
		}

		<-ch

		if err != nil {
			log.Println(err)
		}
	case <-done:			//session done
		a.Close()
		b.Close()
		<-ch
		<-ch
	}

	return err
}

func ioCopyUntilError(a, b io.ReadWriteCloser, ch chan error) {
	_, err := io.Copy(a, b)
	ch <- err
}