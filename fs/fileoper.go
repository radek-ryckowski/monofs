package monofs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"syscall"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	monofile "github.com/radek-ryckowski/monofs/fs/file"
	"github.com/radek-ryckowski/monofs/fs/fsdb"
	"github.com/radek-ryckowski/monofs/utils"
)

// CreateFile Create a new file.
func (fs *Monofs) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) error {
	// Create a new inode.
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	i, err := fs.GetInode(op.Parent, op.Name, true)
	if err == nil {
		op.Entry.Child = i.ID()
		op.Entry.Attributes = i.Attrs.InodeAttributes
		return nil
	}
	t := fs.Clock.Now()
	// add to sha256 name of file current time and some random string
	sha256 := sha256.New()
	sha256.Write([]byte(op.Name))
	sha256.Write([]byte(t.String()))
	sha256.Write([]byte(utils.RandString(32)))
	inode := fs.NewInode(op.Parent, op.Name,
		fsdb.InodeAttributes{
			Hash: fmt.Sprintf("%x.%s", sha256.Sum(nil), fs.CurrentSnapshot),
			InodeAttributes: fuseops.InodeAttributes{
				Size:  0,
				Nlink: 1,
				Mode:  op.Mode,
				Rdev:  0,
				Uid:   fs.uid,
				Gid:   fs.gid,
				Atime: t,
				Mtime: t,
				Ctime: t,
			},
		},
	)
	if err := fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("CreateFile(%d:%s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	fsHandle, err := monofile.New(fs.Name, inode.ID(), inode.Attrs.Hash, fs.localDataPath)
	if err != nil {
		fs.log.Errorf("CreateFile(%d:%s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	fs.fileHandles[op.Handle] = fsHandle
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// CreateLink Create a new link.
func (fs *Monofs) CreateLink(
	ctx context.Context,
	op *fuseops.CreateLinkOp) error {
	fs.log.Debugf("CreateLink(%d:%s)", op.Parent, op.Name)
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	iattr, err := fs.metadb.GetFsdbInodeAttributes(uint64(op.Target))
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("CreateLink(GetInodeAttr) %d: %v", op.Target, err)
		return fuse.EIO
	}
	inode := fsdb.NewInode(uint64(op.Target), uint64(op.Parent), op.Name,
		fsdb.InodeAttributes{
			Hash:            iattr.Hash,
			ParentID:        iattr.ParentID,
			InodeAttributes: iattr.InodeAttributes,
		})
	inode.Attrs.Nlink++
	if err = fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("CreateLink(AddInode)(%d:%s): %v", op.Target, op.Name, err)
		return fuse.EIO
	}
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// CreateSymlink Create a new symlink.
func (fs *Monofs) CreateSymlink(
	ctx context.Context,
	op *fuseops.CreateSymlinkOp) error {
	fs.log.Debugf("CreateSymlink(%s:%s)", op.Parent, op.Name)
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	t := fs.Clock.Now()
	inode := fs.NewInode(op.Parent, op.Name,
		fsdb.InodeAttributes{
			Hash: op.Target,
			InodeAttributes: fuseops.InodeAttributes{
				Size:  0,
				Nlink: 1,
				Mode:  0777 | os.ModeSymlink,
				Rdev:  0,
				Uid:   fs.uid,
				Gid:   fs.gid,
				Ctime: t,
				Mtime: t,
				Atime: t,
			},
		})
	if err := fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("CreateSymlink(AddInode)(%s:%s): %v", op.Target, op.Name, err)
		return fuse.EIO
	}
	op.Entry.Child = inode.ID()
	op.Entry.Attributes = inode.Attrs.InodeAttributes
	return nil
}

// TODO ReadSymlink
func (fs *Monofs) ReadSymlink(
	ctx context.Context,
	op *fuseops.ReadSymlinkOp) error {
	fs.log.Debugf("ReadSymlink(%d:%s)", op.Inode, op.Target)
	attr, err := fs.metadb.GetFsdbInodeAttributes(uint64(op.Inode))
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("ReadSymlink(GetInode): %v", err)
		return fuse.EIO
	}
	if fsdb.InodeDirentType(attr.InodeAttributes.Mode) == fuseutil.DT_Link {
		//TODO check if hash is not a sha256 hash
		op.Target = attr.Hash
	}
	return nil
}

// Rename rename a file or directory
func (fs *Monofs) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) error {
	fs.log.Debugf("Rename(%d:%s -> %d:%s)", op.OldParent, op.OldName, op.NewParent, op.NewName)
	// Look up the source inode.
	fs.fsHashLock.Lock(op.OldParent)
	inode, err := fs.GetInode(op.OldParent, op.OldName, true)
	if err != nil {
		fs.fsHashLock.Unlock(op.OldParent)
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("Rename(GetInode) (%s:%d): %v", op.OldParent, op.OldName, err)
		return fuse.EIO
	}
	// Remove it from the source directory.
	err = fs.DeleteInode(inode, false)
	fs.fsHashLock.Unlock(op.OldParent)
	if err != nil {
		fs.log.Errorf("Rename(DeleteInode)(%d:%s): %v", inode.ParentID, inode.Name, err)
		return fuse.EIO
	}

	fs.fsHashLock.Lock(op.NewParent)
	defer fs.fsHashLock.Lock(op.NewParent)
	// Add it to the target directory.
	inode.SetParent(op.NewParent)
	inode.SetName(op.NewName)
	inode.SetAttrsParent(op.NewParent)
	t := fs.Clock.Now()
	inode.Attrs.Mtime = t
	inode.Attrs.Atime = t
	if err = fs.AddInode(inode, true); err != nil {
		fs.log.Errorf("Rename(AddInode)(%d:%s): %v", inode.ParentID, inode.Name, err)
		return fuse.EIO
	}
	return nil
}

// Unlink remove a file or directory
func (fs *Monofs) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) error {
	fs.fsHashLock.Lock(op.Parent)
	defer fs.fsHashLock.Unlock(op.Parent)
	// Look up the source inode.
	inode, err := fs.GetInode(op.Parent, op.Name, true)
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("Unlink(GetInode)(%d:%s): %v", op.Parent, op.Name, err)
		return fuse.EIO
	}
	// check if it's directory
	if inode.Attrs.Mode&os.ModeDir == os.ModeDir {
		return syscall.EISDIR
	}
	if inode.Attrs.Mode&os.ModeSymlink != os.ModeSymlink {
		if inode.Attrs.Nlink > 1 {
			inode.Attrs.Nlink--
			if err = fs.CreateInodeAttrs(inode); err != nil {
				fs.log.Errorf("Unlink(CreateInodeAttrs)(%d): %v", inode.ID(), err)
				return fuse.EIO
			}
		} else {
			if err := fs.DeleteInode(inode, false); err != nil {
				fs.log.Errorf("Unlink(DeleteInode)(%d:%s): %v", inode.ParentID, inode.Name, err)
				return fuse.EIO
			}
		}
	}
	return nil
}

// OpenFile open a file
func (fs *Monofs) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) error {
	a, err := fs.metadb.GetFsdbInodeAttributes(uint64(op.Inode))
	if err != nil {
		if err == fsdb.ErrNoSuchInode {
			return fuse.ENOENT
		}
		fs.log.Errorf("OpenFile(GetInodeAttrs)(%d): %v", op.Inode, err)
		return fuse.EIO
	}
	// check if it's file and have emtpty hash - error
	if a.GetHash() == "" && fsdb.InodeDirentType(a.InodeAttributes.Mode) == fuseutil.DT_File {
		fs.log.Errorf("OpenFile(GetInodeAttrs)(%d): hash is empty", op.Inode)
		return fuse.EIO
	}
	// Create a handle.
	fsh, err := monofile.New(fs.Name, op.Inode, a.GetHash(), fs.localDataPath)
	if err != nil {
		fs.log.Errorf("OpenFile(NewFileHandle)(%d): %v", op.Inode, err)
		return fuse.EIO
	}
	fs.fileHandles[op.Handle] = fsh
	return nil
}

// ReadFile read a file
func (fs *Monofs) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) error {
	var err error
	// Look up the file.
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Read the file.
	op.BytesRead, err = handle.ReadAt(op.Dst, op.Offset, op.Size)
	return err
}

// WriteFile write a file
func (fs *Monofs) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) error {
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Write the file.
	_, err := handle.WriteAt(op.Data, int64(op.Offset))
	return err
}

// FlushFile flush a file
func (fs *Monofs) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) error {
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Flush the file.
	return handle.Sync()
}

// ReleaseFileHandle release a file handle
func (fs *Monofs) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) error {
	// Release the file.
	delete(fs.fileHandles, op.Handle)
	return nil
}

// SyncFile sync a file
func (fs *Monofs) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) error {
	handle, ok := fs.fileHandles[op.Handle]
	if !ok {
		return fuse.EINVAL
	}
	// Flush the file.
	return handle.Sync()
}
