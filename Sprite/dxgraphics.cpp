// dxgraphics.cpp - Direct3D framework source code file
#include <d3d9.h>
#include <d3dx9.h>
#include "dxgraphics.h"

//variable declarations
LPDIRECT3D9 d3d = NULL;
LPDIRECT3DDEVICE9 d3ddev = NULL;
LPDIRECT3DSURFACE9 backbuffer = NULL;
int Init_Direct3D(HWND hwnd, int width, int height, int fullscreen)
{
	//initialize Direct3D
	d3d = Direct3DCreate9(D3D_SDK_VERSION);
	if (d3d == NULL)
	{
		MessageBox(hwnd, L"Error initializing Direct3D", L"Error", MB_OK);
		return 0;
	}
	//set Direct3D presentation parameters
	D3DPRESENT_PARAMETERS d3dpp;
	ZeroMemory(&d3dpp, sizeof(d3dpp));
	d3dpp.Windowed = (!fullscreen);
	d3dpp.SwapEffect = D3DSWAPEFFECT_COPY;
	d3dpp.BackBufferFormat = D3DFMT_X8R8G8B8;
	d3dpp.BackBufferCount = 1;
	d3dpp.BackBufferWidth = width;
	d3dpp.BackBufferHeight = height;
	d3dpp.hDeviceWindow = hwnd;
	//create Direct3D device
	d3d->CreateDevice(
		D3DADAPTER_DEFAULT,
		D3DDEVTYPE_HAL,
		hwnd,
		D3DCREATE_SOFTWARE_VERTEXPROCESSING,
		&d3dpp,
		&d3ddev);
	if (d3ddev == NULL)
	{
		MessageBox(hwnd, L"Error creating Direct3D device", L"Error", MB_OK);
		return 0;
	}
	//clear the backbuffer to black
	d3ddev->Clear(0, NULL, D3DCLEAR_TARGET, D3DCOLOR_XRGB(0, 0, 0), 1.0f, 0);
	//create pointer to the back buffer
	d3ddev->GetBackBuffer(0, 0, D3DBACKBUFFER_TYPE_MONO, &backbuffer);
	return 1;
}
LPDIRECT3DSURFACE9 LoadSurface(LPCWSTR filename, D3DCOLOR transcolor)
{
	LPDIRECT3DSURFACE9 image = NULL;  // Khởi tạo surface được sử dụng để lưu trữ dữ liệu từ file ảnh và trả về

	D3DXIMAGE_INFO info; // Khởi tạo biến để lưu trữ thông tin về file ảnh

	HRESULT result; // Khởi tạo biến để lưu trữ kết quả trả về của các hàm

	// Lấy thông tin về file ảnh, nếu thất bại trả về NULL
	result = D3DXGetImageInfoFromFile(filename, &info);
	if (!SUCCEEDED(result))
	{
		return NULL;
	}

	// Tạo surface để lưu trữ dữ liệu từ file ảnh
	result = d3ddev->CreateOffscreenPlainSurface(info.Width, info.Height, D3DFMT_X8R8G8B8, D3DPOOL_DEFAULT, &image, NULL);

	if (!SUCCEEDED(result))
	{
		return NULL;
	}

	// Load dữ liệu từ file ảnh lên surface đã tạo
	result = D3DXLoadSurfaceFromFile(
		image, // con trỏ tới surface đích, được tạo ra trước đó để lưu trữ dữ liệu từ file ảnh
		NULL, // con trỏ tới palette nếu cần, ở đây không sử dụng
		NULL, // con trỏ tới hình chữ nhật (RECT) xác định phần của surface mà sẽ được điền vào, ở đây không sử dụng nên để NULL
		filename, // đường dẫn tới file ảnh cần load lên surface
		NULL, // con trỏ tới hình chữ nhật (RECT) xác định phần của file ảnh mà sẽ được load lên surface, ở đây không sử dụng nên để NULL
		D3DX_DEFAULT, // giá trị mặc định của filter, dùng để áp dụng cho việc scale ảnh, ở đây dùng giá trị mặc định
		transcolor, // giá trị màu sắc của key color, được sử dụng để loại bỏ một màu sắc cụ thể khỏi ảnh khi load, ở đây không sử dụng nên để 0
		NULL // con trỏ tới D3DXIMAGE_INFO structure chứa thông tin về ảnh được load, ở đây không sử dụng nên để NULL
	);

	// Nếu load dữ liệu không thành công thì trả về NULL
	if (!SUCCEEDED(result))
	{
		return NULL;
	}

	// Nếu load dữ liệu thành công, trả về surface lưu trữ dữ liệu đã được load
	return image;
}
